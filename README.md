# Summary

The purpose of this service is to read sierra bib/item ids from the SierraBibRetriever/SierraItemRetriever stream, retrieve those records from the sierra-wrapper (https://www.npmjs.com/package/sierra-wrapper), and publish them to the BibPostRequest/ItemPostRequest Stream.

# Setup
1. Clone the repo to your local machine
2. Assuming you have node installed properly in your machine, run npm install
3. Ask a coworker for appropriate environment.json files

# Testing
Test suite is currently broken.

To run locally, event.json is also added as part of the repo. You can also generate events
using kinesify-data (see kinesify-data.js for usage)

You can run:

```
NODE_CONFIG_ENV=[development|production] ./node_modules/.bin/node-lambda run [-j newEvent.json] -f config/[item|bib]-[environment]
```

If everything is working you will get bunyan logs up until the lambda attempts to write to the stream, where it
will fail.

# Deployment
To deploy, run:

```
npm run deploy-[bib|item]-[development|production]
```
