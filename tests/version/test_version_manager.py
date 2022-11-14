# Copyright 2022 Avaiga Private Limited
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
# an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.

from src.taipy.core._version._version_manager import _VersionManager


def test_save_and_get_version_entity(tmpdir):
    _VersionManager._repository.base_path = tmpdir

    assert len(_VersionManager._get_all()) == 0

    # _VersionManager._set(cycle)

    # cycle_1 = _VersionManager._get(cycle.id)

    # assert cycle_1.id == cycle.id
    # assert cycle_1.name == cycle.name
    # assert cycle_1.properties == cycle.properties
    # assert cycle_1.creation_date == cycle.creation_date
    # assert cycle_1.start_date == cycle.start_date
    # assert cycle_1.end_date == cycle.end_date
    # assert cycle_1.frequency == cycle.frequency

    # assert len(_VersionManager._get_all()) == 1
    # assert _VersionManager._get(cycle.id) == cycle
    # assert _VersionManager._get(cycle.id).name == cycle.name
    # assert isinstance(_VersionManager._get(cycle.id).creation_date, datetime)
    # assert _VersionManager._get(cycle.id).creation_date == cycle.creation_date
    # assert _VersionManager._get(cycle.id).frequency == Frequency.DAILY

    # cycle_2_id = CycleId("cycle_2")
    # assert _VersionManager._get(cycle_2_id) is None

    # cycle_3 = Cycle(
    #     Frequency.MONTHLY,
    #     {},
    #     creation_date=current_datetime,
    #     start_date=current_datetime,
    #     end_date=current_datetime,
    #     name="bar",
    #     id=cycle_1.id,
    # )
    # _VersionManager._set(cycle_3)

    # cycle_3 = _VersionManager._get(cycle_1.id)

    # assert len(_VersionManager._get_all()) == 1
    # assert cycle_3.id == cycle_1.id
    # assert cycle_3.name == cycle_3.name
    # assert cycle_3.properties == cycle_3.properties
    # assert cycle_3.creation_date == current_datetime
    # assert cycle_3.start_date == current_datetime
    # assert cycle_3.end_date == current_datetime
    # assert cycle_3.frequency == cycle_3.frequency
