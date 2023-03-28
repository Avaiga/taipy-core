# Copyright 2023 Avaiga Private Limited
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
# an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.

from queue import SimpleQueue
from typing import Optional
from uuid import uuid4

from .registration_id import RegistrationId
from .topic import Topic


class Registration:

    _ID_PREFIX = "REGISTRATION"
    __SEPARATOR = "_"

    def __init__(self, entity_type: Optional[str], entity_id: Optional[str], operation, attribute_name):

        self.register_id: str = self._new_id()
        self.topic: Topic = Topic(entity_type, entity_id, operation, attribute_name)
        self.queue: SimpleQueue = SimpleQueue()  # TODO: should we allow user to provide their own queue?

    @staticmethod
    def _new_id() -> RegistrationId:
        """Generate a unique scenario identifier."""
        return RegistrationId(Registration.__SEPARATOR.join([Registration._ID_PREFIX, str(uuid4())]))