# Sierra Retriever

The purpose of this service is to read sierra bib/item ids from the SierraBibRetriever/SierraItemRetriever stream, retrieve those records from the sierra-wrapper (https://www.npmjs.com/package/sierra-wrapper), and publish them to the BibPostRequest/ItemPostRequest Stream.

## Setup

1. Clone the repo to your local machine
2. Assuming you have node installed properly in your machine, run npm install
3. Ask a coworker for appropriate environment.json files

### Configuration files

We use several different configuration files with different roles:

1. `config/[bib|item]-[environment].env`:

  Set environment variables. These are versioned and should not require changes

2. `config/[environment].json`:

  Overwrite default.json with appropriate secrets and endpoints, see default.json for correct format and sample data. Used by [the config module](https://www.npmjs.com/package/config) to read variables for runtime configuration

3. `.env`:

  Used by node-lambda to configure AWS lambda environment (subnets, security groups, region, etc). Largely overwritten by specific scripts in package.json

## GIT Workflow

We follow a [feature-branch](https://www.atlassian.com/git/tutorials/comparing-workflows/feature-branch-workflow) workflow. Our branches, ordered from least-stable to most stable are:

| branch        | AWS account      |
|:--------------|:-----------------|
| `development` | nypl-sandbox     |
| `qa`          | nypl-digital-dev |
| `master`      | nypl-digital-dev |

If you need to introduce/update the application code, you `SHOULD`:

* Create feature branches off the `development` branch.
* Send a PR pointing to the `development` branch upon completion of feature branch.
* Once the PR is approved, it should be merged into the `development` branch.
* When a release is to be deployed, the `development` branch will be merged into `qa`.
* Upon feeling happy with the results in QA, merge `qa` into `master`.

## Testing

Test suite is currently broken.

To run locally, event.json is also added as part of the repo. You can also generate events
using kinesify-data (see kinesify-data.js for usage)

You can run:

```
NODE_CONFIG_ENV=[development|qa|production] ./node_modules/.bin/node-lambda run [-j newEvent.json] -f config/[item|bib]-[environment]
```

If everything is working you will get bunyan logs up until the lambda attempts to write to the stream, where it
will fail.


## Deployment

To deploy, run:

```
npm run deploy-[bib|item]-[development|qa|production]
```
