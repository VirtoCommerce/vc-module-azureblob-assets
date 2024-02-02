# Virto Commerce Azure Blob Storage Assets Module
[![CI status](https://github.com/VirtoCommerce/vc-module-azureblob-assets/workflows/Module%20CI/badge.svg?branch=dev)](https://github.com/VirtoCommerce/vc-module-azureblob-assets/actions?query=workflow%3A"Module+CI") [![Quality gate](https://sonarcloud.io/api/project_badges/measure?project=VirtoCommerce_vc-module-azureblob-assets&metric=alert_status&branch=dev)](https://sonarcloud.io/dashboard?id=VirtoCommerce_vc-module-azureblob-assets) [![Reliability rating](https://sonarcloud.io/api/project_badges/measure?project=VirtoCommerce_vc-module-azureblob-assets&metric=reliability_rating&branch=dev)](https://sonarcloud.io/dashboard?id=VirtoCommerce_vc-module-azureblob-assets) [![Security rating](https://sonarcloud.io/api/project_badges/measure?project=VirtoCommerce_vc-module-azureblob-assets&metric=security_rating&branch=dev)](https://sonarcloud.io/dashboard?id=VirtoCommerce_vc-module-azureblob-assets) [![Sqale rating](https://sonarcloud.io/api/project_badges/measure?project=VirtoCommerce_vc-module-azureblob-assets&metric=sqale_rating&branch=dev)](https://sonarcloud.io/dashboard?id=VirtoCommerce_vc-module-azureblob-assets)

## Overview
Azure Blob Storage Assets module provide integration with [Azure Blob Storage](https://azure.microsoft.com/en-us/products/storage/blobs).

## Settings
1. Open **appsettings.json** for the Virto Commerce Platform instance.
2. Navigate to the **Assets** node:
```json
    "Assets": {
        "Provider": "AzureBlobStorage",
        "AzureBlobStorage": {
            "ConnectionString": "",
            "CdnUrl": "",
            "AllowBlobPublicAccess": true
        }
    }
```
3. Modify the following settings:
    - Set the **Provider** value to **AzureBlobStorage**.
    - Provide **ConnectionString** in case you are going to use the **AzureBlobStorage** implementation option.
    - Set up **AllowBlobPublicAccess** to `false`, if create private container by default. By default, `true`.

## Documentation
* [Assets Module Documentation](https://virtocommerce.com/docs/latest/modules/assets/)
* [View on GitHub](docs/index.md)


## References

* Deploy: https://virtocommerce.com/docs/latest/developer-guide/deploy-module-from-source-code/
* Installation: https://www.virtocommerce.com/docs/latest/user-guide/modules/
* Home: https://virtocommerce.com
* Community: https://www.virtocommerce.org
* [Download Latest Release](https://github.com/VirtoCommerce/vc-module-azureblob-assets/releases/latest)

## License

Copyright (c) Virto Solutions LTD.  All rights reserved.

Licensed under the Virto Commerce Open Software License (the "License"); you
may not use this file except in compliance with the License. You may
obtain a copy of the License at

http://virtocommerce.com/opensourcelicense

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied.

