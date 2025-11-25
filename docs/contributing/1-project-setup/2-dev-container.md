### Dev Container

ParquetSharp can be built and tested within a [dev container](https://containers.dev). This is a probably the easiest way to get started, as all the C++ dependencies are prebuilt into the container image.

#### Visual Studio Code

If you want to work locally in [Visual Studio Code](https://code.visualstudio.com), all you need is to have [Docker](https://docs.docker.com/get-docker/) and the [Dev Containers extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers) installed.

Simply open up your copy of ParquetSharp in VS Code and click "Reopen in container" when prompted. Once the project has been opened, you can follow the GitHub Codespaces instructions above.

<details>
<summary>Podman and SELinux workarounds</summary>
Using the dev container on a Linux system with podman and SELinux requires some workarounds.

You'll need to edit `.devcontainer/devcontainer.json` and add the following lines:

```json
  "remoteUser": "root",
  "containerUser": "root",
  "workspaceMount": "",
  "runArgs": ["--volume=${localWorkspaceFolder}:/workspaces/${localWorkspaceFolderBasename}:Z"],
  "containerEnv": { "VCPKG_DEFAULT_BINARY_CACHE": "/home/vscode/.cache/vcpkg/archives" }
```

This configures the container to run as the root user,
because when you run podman as a non-root user your user id is mapped
to root in the container, and files in the workspace folder will be owned by root.

The workspace mount command is also modified to add the `:Z` suffix,
which tells podman to relabel the volume to allow access to it from within the container.

Finally, setting the `VCPKG_DEFAULT_BINARY_CACHE` environment variable
makes the root user in the container use the vcpkg cache of the vscode user.
</details>

#### GitHub Codespaces

If you have a GitHub account, you can simply open ParquetSharp in a new GitHub Codespace by clicking on the green "Code" button at the top of this page.

Choose the "unspecified" CMake kit when prompted and let the C++ configuration run. Once done, you can build the C++ code via the "Build" button in the status bar at the bottom.

You can then build the C# code by right-clicking the ParquetSharp solution in the Solution Explorer on the left and choosing "Build". The Test Explorer will then get populated with all the C# tests too.

#### CLI

If the CLI is how you roll, then you can install the [Dev Container CLI](https://github.com/devcontainers/cli) tool and issue the following command in the your copy of ParquetSharp to get up and running:

```bash
devcontainer up
```

Build the C++ code and run the C# tests with:

```bash
devcontainer exec ./build_unix.sh
devcontainer exec dotnet test csharp.test
```