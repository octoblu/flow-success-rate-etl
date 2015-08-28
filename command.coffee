url = require 'url'
ElasticSearch = require 'elasticsearch'
async = require 'async'
_ = require 'lodash'
request = require 'request'
QUERY = require './query.json'

class Command
  constructor: ->
    sourceElasticsearchUrl       = process.env.SOURCE_ELASTICSEARCH_URL ? 'localhost:9200'
    @destinationElasticsearchUrl = process.env.DESTINATION_ELASTICSEARCH_URL ? 'localhost:9200'

    @sourceElasticsearch      = new ElasticSearch.Client host: sourceElasticsearchUrl

  run: =>
    @search QUERY, (error, result) =>
      throw error if error?

      deployments = @process @normalize result
      async.each deployments, @update, (error) =>
        throw error if error?
        process.exit 0

  update: (deployment, callback) =>
    uri = url.format
      protocol: 'http'
      host: @destinationElasticsearchUrl
      pathname: "/flow_start_history/event/#{deployment.deploymentUuid}"

    request.put uri, json: deployment, (error, response, body) =>
      return callback error if error?
      return callback new Error(JSON.stringify body) if response.statusCode >= 300
      callback null

  search: (body, callback=->) =>
    @sourceElasticsearch.search({
      index: 'device_status_flow'
      type:  'event'
      search_type: 'count'
      body:  body
    }, callback)

  normalize: (result) =>
    buckets = result.aggregations.flowStart.group_by_deploymentUuid.buckets
    _.map buckets, (bucket) =>
      {
        deploymentUuid: bucket.key
        beginTime: bucket.beginRecord.beginTime.value
        endTime:   bucket.endRecord.endTime.value
        workflow: 'flow-start'
      }

  process: (deployments) =>
    _.map deployments, (deployment) =>
      {beginTime, endTime} = deployment

      elapsedTime = null
      elapsedTime = endTime - beginTime if endTime?

      _.extend {}, deployment, {
        elapsedTime: elapsedTime
        success: endTime?
      }

command = new Command()
command.run()
