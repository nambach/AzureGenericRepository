# AzureGenericRepository
Generic repository pattern with Azure Table Service as cloud service

This pattern has two type of data objects: Model and Entity.

Model is the program classes/entities, which represents full features and properties, while Entity is the one that's stored on cloud service.

To use the pattern, you should provide Entity which extends the GenericEntity, and your Model which extends GenericModel. 
You may also provide two converting methods that converts your Entity into Model back and forth.
Finally, create your repository by doing these steps:
  1. Create repository interface implementing GenericBusiness
  2. Create repository class that implements the above interface and extends GenericBusinessImpl
