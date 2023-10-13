# Multinode's Python clients

There are two separate clients. 
 - `multinode` - used for defining and managing multinode deployments
 - `multinode-external` - used for interacting with multinode deployments from external 
   code (e.g., from an API hosted on Google Cloud Run)

They also share a significant amount of code that's defined in the `multinode-shared`
directory.

## Contributing
### Initializing clients
To be able to work the code needs:
  1. Installed poetry-multiproject-plugin
  2. Auto-generated control-plane client code
  3. Installed python dependencies 

Both of these can be achieved by running the init script:
```commandline
bash scripts/init.sh
```

It requires control plane schema to be available under the 
`api-schemas/control-plane.json` path.

