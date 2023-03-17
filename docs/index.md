# Overview

Azure Blob Storage Assets module provide integration with [Azure Blob Storage](https://azure.microsoft.com/en-us/products/storage/blobs).

## Settings
1. Open **appsettings.json** for the Virto Commerce Platform instance.
2. Navigate to the **Assets** node:
```json
    "Assets": {
        "Provider": "AzureBlobStorage",
        "AzureBlobStorage": {
            "ConnectionString": "",
            "CdnUrl": ""
        }
    }
```
3. Modify the following settings:
    - Set the **Provider** value to **AzureBlobStorage**
    - Provide **ConnectionString** in case you are going to use the **AzureBlobStorage** implementation option