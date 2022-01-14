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

Test suite is currently broken and has near zero coverage.

In lieu of a working test suite, you can validate the app by generating test events, invoking the lambda locally on those events, and examining the result.

### Generating events

See `kinesify-data.js` for generating event files containing arbitrary ids.

For example to generate an event file containing three bib ids:
```
node kinesify-data 19149236,12579650,16005621 SierraBibRetrievalRequest
```

To generate a file with three item ids:
```
node kinesify-data 19149236,12579650,16005621 SierraItemRetrievalRequest
```

### Testing events locally using sam

To test bibs, first monitor the `BibPostRequest-qa` stream using [stream-listener](https://github.com/NYPL-discovery/stream-listener)

```
node stream-listen --stream BibPostRequest-qa --decode --profile nypl-digital-dev --schema BibPostRequest --iterator LATEST
```

While that's running, build an `newEvent.json` containing bib ids and run:

```
sam local invoke -t sam.bib-qa.yml -e newEvent.json
```

Verify that three records are written to `BibPostRequest-qa` and they look plausibly like the correct records.

Repeat by monitoring `ItemPostRequest-qa`, using `sam.item-qa.yml` and an event file with item ids to verify the app can handle items.

### Running locally using node-lambda

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
