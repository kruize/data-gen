#############################################################################
#    Copyright (c) 2026 Red Hat, IBM Corporation and others.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#############################################################################
class Constants:
    KRUIZE_TEAM_NAMES = [
        "dinakar",
        "rebecca",
        "rashmi",
        "bhakta",
        "kusuma",
        "chandrakala",
        "pinky",
        "vinay",
        "saad",
        "bhanvi",
        "shreya",
        "shekhar",
        "nick",
        "bharath"
    ]

    NAMESPACE = "namespace"

    INTERVAL_CHOICES = ["1s", "5s", "15s", "30s", "60s"]

    class InputConsts:
        DEFAULT_NUM_NAMESPACES = 183
        DEFAULT_MIN_DEPLOYMENTS = 1
        DEFAULT_MAX_DEPLOYMENTS = 25
        DEFAULT_MIN_REPLICAS = 1
        DEFAULT_MAX_REPLICAS = 10
        DEFAULT_INTERVAL = "30s"
        DEFAULT_PRE_DAYS = 15
        DEFAULT_POST_DAYS = 15
        DEFAULT_CONFIG_NAME = "default"

    class ResourceConstraints:
        ZERO_VAL = 0.00

        MIN_CPU = 0.01
        MAX_CPU = 8.00

        MIN_MIN_CPU = MIN_CPU
        MAX_MIN_CPU = 0.1

        MIN_AVG_CPU = 0.50
        MAX_AVG_CPU = 3.50

        MIN_MAX_CPU = 2.00
        MAX_MAX_CPU = MAX_CPU

        IDLE_CPU_MIN = 0.00001
        IDLE_CPU_MAX = 0.0001

        MIN_MEMORY = 50.00
        MAX_MEMORY = 4000.00

        MIN_MIN_MEMORY = MIN_MEMORY
        MAX_MIN_MEMORY = 150.00

        MIN_AVG_MEMORY = 200.00
        MAX_AVG_MEMORY = 2000.00

        MIN_MAX_MEMORY = 1500.00
        MAX_MAX_MEMORY = MAX_MEMORY

        MIN_GPU = 1.00
        MAX_GPU = 100.00

        MIN_MIN_GPU = MIN_GPU
        MAX_MIN_GPU = 25.00

        MIN_AVG_GPU = 25.00
        MAX_AVG_GPU = 65.00

        MIN_MAX_GPU = 65.00
        MAX_MAX_GPU = MAX_GPU
