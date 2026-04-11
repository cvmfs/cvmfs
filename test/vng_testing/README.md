# VNG Testing

## Objective

VNG Testing provides a framework for automating and validating kernel and filesystem-related tests in a virtualized environment. It is designed to facilitate testing with Linux kernels and CernVM-FS images, ensuring reproducibility and isolation.

## Project Structure

```
vng_testing/
├── build_kernel.sh         # Script to build custom kernels
├── cvmfs.img               # Virtual disk image used for mounting into the guest
├── README.md
├── run.sh                  # Boots VM and runs test/run.sh inside it
├── configs/
│   └── kernel_config.conf  # Custom kernel build configuration
├── kernel/                 # Holds bzImages for kernels
│   ├── v5.11/
│   │   └── bzImage         # Example Kernel image version 5.11
└── tests/                  # Tests are here
```

## How It Works

1. **Kernel Preparation:**
   Use `build_kernel.sh` to build custom kernels with additional config options as specified in `configs/kernel_config.conf`. Kernels will be fetched from `KERNEL_BASE_URL` and will be kept under `kernel/vX.YZ`.

2. **Test Orchestration:**
   The `run.sh` script launches a virtual machine via virtme-ng, boots it with the selected kernel, and mounts the `cvmfs.img` filesystem. Inside the VM it performs guest-specific setup (mounting tmpfs for `/cvmfs`, mounting the cache disk) and then delegates to `test/run.sh` — the standard integration test runner — with all arguments forwarded.

3. **Test Discovery and Execution:**
   `test/run.sh` discovers and executes each test suite under `src/`. Each test is organized in its own directory with a `main` script or binary. The full test runner provides timeouts, xUnit output, colored status, and environment setup/cleanup.

4. **Extensibility:**
   New tests can be added by creating a new subdirectory under `tests/` with a `main` executable or script and can later be merged in `../src` after a successful run.

## Dependencies

- virtme-ng
- virtiofsd (optional, but recommended for faster boot times) - visit [here](https://gitlab.com/virtio-fs/virtiofsd) for installation guide
- QEMU (used by virtme-ng)
- Bash shell
- GNU compiler (for building kernels and C programs - visit [here](https://docs.kernel.org/process/changes.html) for a comprehensive list specific to the build machine)

## Configs
A level of configuration can be achieved by exporting the environment variables to desired value to provide control.
```sh
KERNEL_BASE_URL="" default: "https://ecsft.cern.ch/dist/cvmfs/caches/kernel/"
DISK_PATH="" default: ./cvmfs.img
DISK_SIZE="" default: 5G
```

## Usage

1. **Build the Kernel (if not already built):**
   ```sh
   ./build_kernel.sh <linux-source-path>
   ```

2. **Run All Tests (default suite):**
   ```sh
   ./run.sh
   ```

3. **Run with specific suite/exclusions:**
   ```sh
   ./run.sh -- -s quick -x src/095-fuser -- src/00* src/01*
   ```
   All arguments after `--` are forwarded directly to `test/run.sh`.

4. **Add or Modify Tests:**
   - Create a new directory under `tests/` and add a `main` script or executable.

---

For further details, consult comments within the scripts and configuration files or reach out to the author.