import numpy as np
import dcor


def median_relative_error(y_true, y_pred, eps=1e-8):
    y_true = np.asarray(y_true)
    y_pred = np.asarray(y_pred)
    return float(np.median(np.abs((y_pred - y_true) / (np.abs(y_true) + eps))))

def dcor_filter(Xm: np.ndarray, y: np.ndarray, threshold: float, min_features: int = 5):
    """
    y can be 1d or 2d. If 2d, average distance correlation across targets.
    """
    if y.ndim == 1:
        corr = np.array([dcor.distance_correlation(Xm[:, i], y) for i in range(Xm.shape[1])])
    else:
        corr = np.array([np.mean([dcor.distance_correlation(Xm[:, i], y[:, j])
                                  for j in range(y.shape[1])])
                         for i in range(Xm.shape[1])])
    mask = corr >= float(threshold)
    if mask.sum() < min_features:
        top_idx = np.argsort(corr)[-min_features:]
        mask[:] = False
        mask[top_idx] = True
    return Xm[:, mask], mask, corr
