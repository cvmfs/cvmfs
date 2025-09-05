# VNG Testing

## Objective

VNG Testing provides a framework for automating and validating kernel and filesystem-related tests in a virtualized environment. It is designed to facilitate testing with Linux kernels and CernVM-FS images, ensuring reproducibility and isolation.

## Project Structure

```
vng_testing/
├── build_kernel.sh         # Script to build custom kernels
├── cvmfs.img               # Virtual disk image used for mounting into the guest
├── README.md
├── run.sh                  # Main script to orchestrate test execution
├── test_functions          # Shared functions for test scripts
├── configs/
│   └── kernel_config.conf  # Custom kernel build configuration
├── guest/
│   └── run_tests.sh        # Script executed inside the guest VM to run tests
├── kernel/                 # Holds bzImages for kernels
│   ├── v5.11/
│   │   └── bzImage         # Example Kernel image version 5.11
├── results/
│   └── test_failures.log   # Log file for failed test cases
└── tests/                  # Tests are here
```

## How It Works

1. **Kernel Preparation:**
   Use `build_kernel.sh` to build custom kernels with additional config options as specified in `configs/kernel_config.conf`. Built kernels are stored under `kernel/vX.YZ`.

2. **Test Orchestration:**
   The `run.sh` script launches a virtual machine via virtme-ng, boots it with the selected kernel, and mounts the `cvmfs.img` filesystem. It then triggers `guest/run_tests.sh` inside the VM.

3. **Test Discovery and Execution:**
   `guest/run_tests.sh` automatically discovers and executes each test suite under `tests/`. Each test is organized in its own directory with a `main` script or binary. Additional files (e.g., C sources) may be included as needed.

4. **Result Collection:**
   Test failures and relevant logs are collected in `results/test_failures.log` for analysis.

5. **Extensibility:**
   New tests can be added by creating a new subdirectory under `tests/` with a `main` executable or script.

## Dependencies

- virtme-ng
- virtiofsd (optional, but recommended for faster boot times) - visit [here](https://gitlab.com/virtio-fs/virtiofsd) for installation guide
- QEMU (used by virtme-ng)
- Bash shell
- GNU compiler (for building kernels and C programs - visit [here](https://docs.kernel.org/process/changes.html) for a comprehensive list specific to the build machine)

## Usage

1. **Build the Kernel (if not already built):**
   ```sh
   ./build_kernel.sh
   ```

2. **Run All Tests:**
   ```sh
   ./run.sh
   ```

3. **Review Test Results:**
   - Check `results/test_failures.log` for details on any failed tests.

4. **Add or Modify Tests:**
   - Create a new directory under `tests/` and add a `main` script or executable.

---

For further details, consult comments within the scripts and configuration files or reach out to the author.