### Common Types and Ctype acclerations for NeuroData

This library is used as a module in neurodata web-services, ndtilecache and ndblaze.

### OSX Build

#### Install GCC that supports OpenMP

- Use brew to install GCC

```bash
brew reinstall gcc --without-multilib
```

This will install it to the /usr/local/bin directory. Homebrew will install it as gcc-<version>so as not to clobber the gcc bundled with XCode. 

#### Setup makefile

- Copy makefile_MAC to makefile

#### Build

```bash
make all
```

#### Makefile help

* Make all implementation
  ```sh
  make all
  ```
* Make test for testing the C implementations
  ```sh
  make test
  ```
* Removes all compiled files
  ```sh
  make clean
  ```
