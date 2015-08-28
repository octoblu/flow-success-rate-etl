_ = require 'lodash'
url = require 'url'
async = require 'async'
moment = require 'moment'
request = require 'request'
ElasticSearch = require 'elasticsearch'

QUERY = require './query.json'

class Command
  constructor: ->
    sourceElasticsearchUrl       = process.env.SOURCE_ELASTICSEARCH_URL ? 'localhost:9200'
    @destinationElasticsearchUrl = process.env.DESTINATION_ELASTICSEARCH_URL ? 'localhost:9200'
    @captureRangeInMinutes = process.env.CAPTURE_RANGE_IN_MINUTES

    @sourceElasticsearch      = new ElasticSearch.Client host: sourceElasticsearchUrl

  run: =>
    @search @query(), (error, result) =>
      throw error if error?

      deployments = @process @normalize result
      async.each deployments, @update, (error) =>
        throw error if error?
        process.exit 0

  query: =>
    return QUERY unless @captureRangeInMinutes?

    captureSince = moment().subtract parseInt(@captureRangeInMinutes), 'minutes'

    query = _.cloneDeep QUERY
    query.aggs.filter_by_timestamp.filter.range._timestamp.gte = captureSince

    return query

  update: (deployment, callback) =>
    uri = url.format
      protocol: 'http'
      host: @destinationElasticsearchUrl
      pathname: "/flow_success_rate/event/"

    request.post uri, json: deployment, (error, response, body) =>
      return callback error if error?
      return callback new Error(JSON.stringify body) if response.statusCode >= 300
      callback null

  search: (body, callback=->) =>
    @sourceElasticsearch.search({
      index: 'flow_deploy_history'
      type:  'event'
      search_type: 'count'
      body:  body
    }, callback)

  normalize: (result) =>
    buckets = result.aggregations.filter_by_timestamp.group_by_workflow.buckets

    flowStart = _.findWhere buckets, key: 'flow-start'
    flowStop = _.findWhere buckets, key: 'flow-stop'

    [
      @normalizeBucket 'flow-start', flowStart
      @normalizeBucket 'flow-stop', flowStop
    ]

  normalizeBucket: (workflow, bucket) =>
    successes = _.find bucket?.group_by_success?.buckets, key: 'T'
    failures  = _.find bucket?.group_by_success?.buckets, key: 'F'

    {
      workflow:  workflow
      successes: successes?.doc_count ? 0
      failures:  failures?.doc_count ? 0
    }

  process: (workflows) =>
    _.map workflows, (workflowObj) =>
      {workflow,successes,failures} = workflowObj

      total = successes + failures

      if total > 0
        successRate = (1.0 * successes) / total
        failureRate = (1.0 * failures) / total
      else
        successRate = 1
        failureRate = 0

      {
        workflow: workflow
        successes: successes
        successRate: successRate
        successPercentage: 100 * successRate
        failures: failures
        failureRate: failureRate
        failurePercentage: 100 * failureRate
        total: total
        captureRangeInMinutes: parseInt(@captureRangeInMinutes)
      }

command = new Command()
command.run()
