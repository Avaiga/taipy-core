# Taipy-Core

## license
Copyright 2022 Avaiga Private Limited

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
the License. You may obtain a copy of the License at

       [http://www.apache.org/licenses/LICENSE-2.0](https://www.apache.org/licenses/LICENSE-2.0.txt)

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.

## What is _Taipy-core_

Taipy is a Python library for creating Business Applications. More information on our
[website](https://www.taipy.io). Taipy is split into multiple repositories including _taipy-core_ to let users
install the minimum they need.

[Taipy Core](https://github.com/Avaiga/taipy-core) mostly includes business-oriented features. It helps users
create and manage business applications and improve analyses capability through time, conditions and hypothesis.

## Installation

Want to install _Taipy Core_? Check out our [`INSTALLATION.md`](INSTALLATION.md) file.

## Contributing

Want to help build _Taipy Core_? Check out our [`CONTRIBUTING.md`](CONTRIBUTING.md) file.

## Code of conduct

Want to be part of the _Taipy Core_ community? Check out our [`CODE_OF_CONDUCT.md`](CODE_OF_CONDUCT.md) file.

## Directory Structure

The files needed to build _taipy-core_ are located in subdirectories that store files as follows:

- `taipy/core`:
    - `taipy/core`:
        - `_repository`: Internal layer for data storage.
        - `_scheduler`: Internal layer for task scheduling and execution.
        - `common`: Shared data structures, types, and functions.
        - `config`: Configuration definition, management and implementation. `config.config.Config` is the main
          entrypoint for configuring a Taipy Core application.
        - `cycle`: Work cycle definition, management and implementation.
        - `data`: Data Node definition, management and implementation.
        - `exceptions`: _taipy-core_ exceptions.
        - `job`: Job definition, management and implementation.
        - `pipeline`: Pipeline definition, management and implementation.
        - `scenario`: Scenario definition, management and implementation.
        - `task`: Task definition, management and implementation.
        - `taipy`: Main entrypoint for _taipy-core_ runtime features.
    - `tests`: Unit tests following the `taipy/core` structure.
- `CODE_OF_CONDUCT.md`: Code of conduct for members and contributors of _taipy-core_.
- `CONTRIBUTING.md`: Instructions to contribute to _taipy-core_.
- `INSTALLATION.md`: Instructions to install _taipy-core_.
- `Pipfile`: TODO.
- `README.md`: Current file.
- `setup.py`: TODO.
- `tox.ini`: Contains test scenarios to be run.
