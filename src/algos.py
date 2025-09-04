from joblib import Parallel, delayed
import dcor
import torch
import time
import warnings
warnings.filterwarnings("ignore")
import numpy as np
import pandas as pd
from typing import Callable, Optional, Tuple, List, Dict
import logging

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MinMaxScaler
from sklearn.neural_network import MLPRegressor
from sklearn.gaussian_process import GaussianProcessRegressor
from sklearn.gaussian_process.kernels import RBF, ConstantKernel as C
from scipy.optimize import minimize, Bounds
from joblib import Parallel, delayed
from utils import median_relative_error, dcor_filter

# -----------------------------------------------------------------------------
# logging setup
# -----------------------------------------------------------------------------
logger = logging.getLogger(__name__)
if not logger.handlers:
    _handler = logging.StreamHandler()
    _fmt = logging.Formatter(
        fmt="%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    _handler.setFormatter(_fmt)
    logger.addHandler(_handler)
    logger.setLevel(logging.INFO)


def mlp_eval(x_batch: np.ndarray, X_full: np.ndarray, Y_full: np.ndarray) -> np.ndarray:
    """
    x_batch shape n by 2: [threshold in 0..1, hidden proxy in 0..1]
    returns n by n_targets with one error per target
    parallelizes across targets for each x
    """
    t0 = time.time()
    x_batch = np.atleast_2d(x_batch)
    out = []
    logger.info("mlp_eval start n=%d targets=%d", x_batch.shape[0], Y_full.shape[1])

    def _eval_one_target(x: np.ndarray, t: int) -> Tuple[float, str]:
        thr = float(np.clip(x[0], 0.0, 1.0))
        hidden = int(np.clip(x[1], 0.0, 1.0) * 190 + 10)

        y_t = Y_full[:, t]
        X_sel, sel_mask, _ = dcor_filter(X_full, y_t, thr)
        if X_sel.shape[1] == 0:
            log = f"[Eval] thr={thr:.3f}, hidden={hidden}, k=0, MRE=1000000.000000"
            return 1e6, log

        X_tr, X_val, y_tr, y_val = train_test_split(X_sel, y_t, test_size=0.2, random_state=0)
        model = MLPRegressor(hidden_layer_sizes=(hidden,),
                             max_iter=100,
                             tol=1e-4,
                             n_iter_no_change=20,
                             random_state=0)
        try:
            model.fit(X_tr, y_tr)
            y_hat = model.predict(X_val)
            err = median_relative_error(y_val, y_hat)
        except Exception:
            logger.exception("mlp_eval target %d fit failed", t)
            err = 1e6

        log = f"[Eval] thr={thr:.3f}, hidden={hidden}, k={(X_sel.shape[1])}, MRE={float(err):.6f}"
        return float(err), log

    for x in x_batch:
        results = Parallel(n_jobs=-1, prefer="processes")(
            delayed(_eval_one_target)(x, t) for t in range(Y_full.shape[1])
        )
        errs, logs = zip(*results)
        for line in logs:
            logger.info(line)
        logger.info("")
        out.append(list(errs))

    dt = time.time() - t0
    logger.info("mlp_eval done in %.3fs", dt)
    return np.asarray(out, dtype=float)


def ref_model_zero(x_batch: np.ndarray, splits: int) -> np.ndarray:
    x_batch = np.atleast_2d(x_batch)
    logger.debug("ref_model_zero called batch=%d splits=%d", x_batch.shape[0], splits)
    return np.zeros((x_batch.shape[0], splits), dtype=float)

# ------------------------------
# acquisition
# ------------------------------
class LCB_AF:
    def __init__(self,
                 model: GaussianProcessRegressor,
                 dim: int,
                 exp_w: float,
                 descale_fn: Callable[[np.ndarray], np.ndarray],
                 refmod: Optional[Callable] = None,
                 args: Tuple = ()): 
        self.model = model
        self.dim = int(dim)
        self.exp_w = float(exp_w)
        self._descale = descale_fn
        self.args = args
        self.refmod = refmod if refmod is not None else (lambda *a, **k: 0.0)

    def __call__(self, x: np.ndarray) -> float:
        x = np.atleast_2d(x.reshape(1, -1) if x.ndim == 1 else x)
        if x.shape[1] != self.dim:
            logger.debug("LCB_AF reshape input from %s to dim %d", str(x.shape), self.dim)
            x = x.reshape(-1, self.dim)
        mu, std = self.model.predict(x, return_std=True)
        mu = mu.reshape(-1)
        std = std.reshape(-1)
        try:
            yref = self.refmod(self._descale(x), *self.args)
            if isinstance(yref, torch.Tensor):
                yref = yref.detach().cpu().numpy()
            yref = np.atleast_2d(yref).reshape(-1)
        except Exception:
            logger.exception("LCB_AF reference model failed")
            yref = np.zeros_like(mu)
        val = yref + mu - self.exp_w * std
        logger.debug("LCB_AF value=%.6f mu=%.6f std=%.6f yref=%.6f", float(val.ravel()[0]), float(mu[0]), float(std[0]), float(yref[0]))
        return float(val.ravel()[0])

# ------------------------------
# VPBO core
# ------------------------------
class BO:
    def __init__(self,
                 distmod: Callable[[np.ndarray, np.ndarray, np.ndarray], np.ndarray],
                 args: Tuple,
                 dist_ref: Dict,
                 ref_args: Tuple,
                 dim: int,
                 bounds: Bounds,
                 kernel: Optional = None,
                 exp_w: float = 2.0,
                 ub: Optional[np.ndarray] = None,
                 lb: Optional[np.ndarray] = None):
        self.distmod = distmod
        self.args = args
        self.dist_ref = dist_ref or {}
        self.ref_args = ref_args
        self.dim = int(dim)
        self.bounds = bounds
        self.kernel = kernel if kernel is not None else C(1.0) * RBF(length_scale=np.ones(dim))
        self.exp_w = float(exp_w)
        self.ub = np.ones(dim) if ub is None else np.asarray(ub, dtype=float)
        self.lb = np.zeros(dim) if lb is None else np.asarray(lb, dtype=float)
        assert self.ub.shape == (self.dim,) and self.lb.shape == (self.dim,)

    def _descale(self, x: np.ndarray) -> np.ndarray:
        x = np.asarray(x, dtype=float)
        m = (self.ub - self.lb) / (self.bounds.ub - self.bounds.lb)
        b = self.ub - m * self.bounds.ub
        return m * x + b

    def optimizer_vpbo(self,
                    trials: int,
                    split_num: int,
                    lim_init: np.ndarray,
                    f_cores: int = 1,
                    af_cores: int = 1,
                    ref_cores: int = 1,
                    x_init: Optional[np.ndarray] = None) -> None:
        start = time.time()
        splits = int(split_num)
        logger.info("optimizer_vpbo start trials=%d splits=%d dim=%d", trials, splits, self.dim)

        # seed rows
        x = lim_init.reshape(1, -1)
        if x_init is None:
            x_split = np.vstack([np.random.uniform(self.bounds.lb, self.bounds.ub, size=(1, self.dim))
                                for _ in range(splits)])
            logger.debug("random seed for splits created shape=%s", str(x_split.shape))
        else:
            x_split = np.repeat(np.atleast_2d(x_init), repeats=splits, axis=0)
            logger.debug("provided seed repeated shape=%s", str(x_split.shape))
        x = np.vstack([x_split, x])

        # batch eval
        def _batched_eval(arr):
            return self.distmod(self._descale(arr), *self.args)

        y_list = Parallel(n_jobs=f_cores)(delayed(_batched_eval)(np.atleast_2d(row)) for row in x)
        y = np.vstack(y_list)
        logger.info("initial eval done n=%d", y.shape[0])

        ref_mod = self.dist_ref.get('distrefmod', None) or (lambda xb, *a: ref_model_zero(xb, splits))
        y_ref_list = Parallel(n_jobs=ref_cores)(
            delayed(lambda r: ref_mod(self._descale(np.atleast_2d(r)), *self.ref_args))(row) for row in x
        )
        y_ref = np.vstack(y_ref_list)

        eps = y - y_ref
        y_bst = np.min(y, axis=0, keepdims=True)

        # one GP per split over full bounds
        models = {}
        bnds_var = {}
        lcbs = {}
        for s in range(splits):
            gp = GaussianProcessRegressor(self.kernel, alpha=1e-6, n_restarts_optimizer=5,
                                        normalize_y=True, random_state=0)
            gp.fit(x, eps[:, s])
            models[s] = gp
            bnds_var[s] = Bounds(self.bounds.lb.copy(), self.bounds.ub.copy())
            lcbs[s] = LCB_AF(gp, self.dim, self.exp_w, self._descale, None, self.ref_args)
        logger.info("GP models initialized count=%d", splits)

        restarts = max(8, 32 // max(1, splits))
        self.time_vp = np.zeros(trials)
        self.time_fvp = np.zeros(trials)
        logger.debug("restarts per split=%d", restarts)

        for it in range(trials):
            t_it = time.time()
            # propose per split on same 2D box
            x_next = np.zeros((splits + 1, self.dim))
            for s in range(splits):
                x0 = np.random.uniform(self.bounds.lb, self.bounds.ub, size=(restarts, self.dim))
                sols = Parallel(n_jobs=af_cores)(
                    delayed(minimize)(lcbs[s], x0_i, method="L-BFGS-B", bounds=bnds_var[s]) for x0_i in x0
                )
                cand = np.array([sol.x for sol in sols])
                vals = np.array([float(sol.fun) for sol in sols])
                x_next[s] = cand[np.argmin(vals)]

            # shared global row
            x_next[-1] = np.mean(x_next[:splits], axis=0)

            # evaluate proposals
            y_next_list = Parallel(n_jobs=f_cores)(delayed(_batched_eval)(np.atleast_2d(row)) for row in x_next)
            y_next = np.vstack(y_next_list)
            y_ref_next_list = Parallel(n_jobs=ref_cores)(
                delayed(lambda r: ref_mod(self._descale(np.atleast_2d(r)), *self.ref_args))(row) for row in x_next
            )
            y_ref_next = np.vstack(y_ref_next_list)

            # update
            x = np.vstack([x, x_next])
            y = np.vstack([y, y_next])
            eps = np.vstack([eps, y_next - y_ref_next])
            y_bst = np.vstack([y_bst, np.min(y_next, axis=0, keepdims=True)])

            for s in range(splits):
                models[s].fit(x, eps[:, s])
                bnds_var[s] = Bounds(self.bounds.lb.copy(), self.bounds.ub.copy())

            self.time_vp[it] = time.time() - start
            self.time_fvp[it] = time.time() - t_it
            logger.info(
                "iter=%d evals_total=%d best_current=%s dt_iter=%.3fs dt_total=%.3fs",
                it + 1,
                x.shape[0],
                np.array2string(np.min(y, axis=0), precision=6, separator=","),
                self.time_fvp[it],
                self.time_vp[it],
            )

        self.model_vp = models
        self.x_vp = self._descale(x)
        self.y_vp = y
        self.y_vpbst = y_bst
        logger.info("optimizer_vpbo done total_evals=%d total_time=%.3fs", x.shape[0], time.time() - start)


def mlp_eval_mean(x_batch: np.ndarray, X_full: np.ndarray, Y_full: np.ndarray) -> np.ndarray:
    logger.debug("mlp_eval_mean start batch=%d", np.atleast_2d(x_batch).shape[0])
    per_target = mlp_eval(x_batch, X_full, Y_full)
    out = np.mean(per_target, axis=1, keepdims=True)
    logger.debug("mlp_eval_mean done")
    return out


def _eval_target_block(x_batch: np.ndarray, X_full: np.ndarray, Y_full: np.ndarray, t: int):
    """
    Compute errs for one target over a batch, return errs and log lines without printing.
    """
    x_batch = np.atleast_2d(x_batch)
    errs = []
    logs = []
    y_t = Y_full[:, t]
    for x in x_batch:
        thr = float(np.clip(x[0], 0.0, 1.0))
        hidden = int(np.clip(x[1], 0.0, 1.0) * 190 + 10)

        X_sel, _, _ = dcor_filter(X_full, y_t, thr)
        if X_sel.shape[1] == 0:
            err = 1e6
        else:
            X_tr, X_val, y_tr, y_val = train_test_split(X_sel, y_t, test_size=0.2, random_state=0)
            model = MLPRegressor(hidden_layer_sizes=(hidden,),
                                 max_iter=100, tol=1e-4, n_iter_no_change=20,
                                 random_state=0)
            try:
                model.fit(X_tr, y_tr)
                y_hat = model.predict(X_val)
                err = median_relative_error(y_val, y_hat)
            except Exception:
                logger.exception("_eval_target_block target %d fit failed", t)
                err = 1e6

        logs.append(f"[Eval] thr={thr:.3f}, hidden={hidden}, k={X_sel.shape[1]}, MRE={float(err):.6f}")
        errs.append(float(err))

    return np.asarray(errs, dtype=float).reshape(-1, 1), logs


def distmod_4(x_batch: np.ndarray, X_full: np.ndarray, Y_full: np.ndarray,
              n_jobs_targets: int = -1, n_jobs_x: int = 1) -> np.ndarray:
    """
    Four column objective. Parallel across targets. Logs printed in x order.
    Avoid nested heavy parallel by keeping n_jobs_x equal to 1 when n_jobs_targets uses many cores.
    """
    t0 = time.time()
    T = Y_full.shape[1]
    logger.info("distmod_4 start n=%d targets=%d", np.atleast_2d(x_batch).shape[0], T)
    # run targets in parallel without printing
    results = Parallel(n_jobs=n_jobs_targets, prefer="processes")(
        delayed(_eval_target_block)(x_batch, X_full, Y_full, t) for t in range(T)
    )
    cols, logs_per_t = zip(*results)

    # print logs grouped by x then by target using logger
    n = cols[0].shape[0]
    for i in range(n):
        for t in range(T):
            logger.info(logs_per_t[t][i])
        logger.info("")

    dt = time.time() - t0
    logger.info("distmod_4 done in %.3fs", dt)
    return np.hstack(cols)
