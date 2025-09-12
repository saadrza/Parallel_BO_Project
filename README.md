# Parallel_BO_Project

Parallelized Bayesian Optimization for evaluating neural network models with feature selection using distance correlation.  

Repository: [Parallel_BO_Project](https://github.com/saadrza/Parallel_BO_Project)

---

## Overview

This project implements **Variance Partitioned Bayesian Optimization (VPBO)** with parallelization. The optimization framework is built around:

- Multi Layer Perceptron regressors with feature selection using **distance correlation**  
- Gaussian Process surrogate models  
- Acquisition functions using **Lower Confidence Bound (LCB)**  
- Parallelization with `joblib` across both targets and candidate evaluation batches  

The core logic is in `algos.py`. Configuration is handled with YAML files. Example usage is provided in Jupyter notebooks.

---

## Project Structure

```
.
├── algos.py          # Core Bayesian Optimization and evaluation functions
├── config.yaml       # Default configuration file
├── config_work.yaml  # Alternative configuration for experiments
├── main.ipynb        # Example notebook for running experiments
└── utils.py          # Helper functions (median_relative_error, dcor_filter, etc.)
```

---

## Key Components

### Evaluation Functions
- **`mlp_eval`**: Trains MLP regressors with selected features, computes median relative error per target  
- **`mlp_eval_mean`**: Aggregates errors across all targets  
- **`distmod_4`**: Multi output objective, evaluates all targets in parallel  

### Acquisition Functions
- **`LCB_AF`**: Lower Confidence Bound acquisition with optional reference model correction  

### Optimizer
- **`BO`**: Variance Partitioned Bayesian Optimizer  
  - Fits Gaussian Process surrogates per partition  
  - Iteratively proposes candidate solutions by minimizing acquisition functions  
  - Supports multiple cores for evaluation and acquisition  

---

## Requirements

Python packages:

- `numpy`  
- `pandas`  
- `scikit-learn`  
- `scipy`  
- `joblib`  
- `torch`  
- `dcor`  
- `pyyaml`  
- `notebook`  

Install with:

```bash
pip install -r requirements.txt
```

*(create a `requirements.txt` with the list above if missing)*

---

## Usage

### Run Example Notebook
Open `main.ipynb` in Jupyter:

```bash
jupyter notebook main.ipynb
```

This shows how to configure and run the optimizer.

### Import as a Module
Example:

```python
from algos import BO, distmod_4
from scipy.optimize import Bounds
import numpy as np

dim = 2
bounds = Bounds([0.0, 0.0], [1.0, 1.0])
optimizer = BO(distmod=distmod_4,
               args=(X_full, Y_full),
               dist_ref={},
               ref_args=(),
               dim=dim,
               bounds=bounds)

optimizer.optimizer_vpbo(trials=10,
                         split_num=4,
                         lim_init=np.array([0.5, 0.5]))
```

---

## Configuration

Settings are controlled through YAML files:

- `config.yaml`: baseline configuration  
- `config_work.yaml`: alternative or experimental setup  

Example fields:

```yaml
trials: 20
splits: 4
f_cores: 2
af_cores: 2
ref_cores: 1
```

These control number of trials, splits, and parallelization.

---

## Logging

Python `logging` with INFO level is used. Output includes:

- Training progress  
- Selected features  
- Errors per target  
- Optimizer iterations and timings  

---

## Contributing

1. Fork the repo  
2. Create a feature branch  
3. Commit and push changes  
4. Open a pull request  

---

## License

MIT License. See LICENSE for details.
