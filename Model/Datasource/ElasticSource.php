<?php

App::uses('HttpSocket', 'Network/Http');


/**
 * Throw this when we are missing an index
 *
 * @package default
 * @author David Kullmann
 */
class MissingIndexException extends CakeException {
	
	protected $_messageTemplate = 'Index "%s" is missing.';
	
}

/**
 * ElasticSource datasource for Elastic Search
 *
 * @package default
 * @author David Kullmann
 */
class ElasticSource extends DataSource {

/**
 * Holds HttpSocket Object
 *
 * @var object
 */
	public $Http;

/**
 * Holds the mappings
 *
 * @var array
 */
	protected $_schema = array();

/**
 * Track if we are in a transaction or not - allows saving multiple models to a document
 *
 * @var boolean
 */
	protected $_transactionStarted = false;

/**
 * If we are in a transaction this is the 'type' for the model indexing
 *
 * @var string
 */
	protected $_type = null;

/**
 * If we are in a transaction this is the id for the model we are indexing
 *
 * @var string
 */
	protected $_id = null;

/**
 * The document for this transaction (for saving more than one model)
 *
 * @var array
 */
	protected $_document = array();

/**
 * Valid operators from DboSource to support
 *
 * @var string
 */
	protected $_filterOps = array('like', 'ilike', 'or', 'not', 'in', 'between', 'regexp', 'similar to');

/**
 * Don't list sources by default, there's still some problems here
 *
 * @var boolean
 */
	protected $_listSources = false;
	
/**
 * Query log
 *
 * @var string
 */
	protected $_queryLog = array(
		'log' => array(),
		'count' => 0,
		'time' => 0
	);

/**
 * Constructor, call the parent and setup Http
 *
 * @param array $config 
 * @author David Kullmann
 */
	public function __construct($config = array()) {
		parent::__construct($config);
		extract($config);
		if (empty($index)) {
			throw new Exception("ElasticSource requires an index is set");
		}
		$scheme = 'http';
		$request = array('uri' => compact('host', 'port', 'scheme'));
		$httpConfig = compact('host', 'port', 'request');
		$this->Http = new HttpSocket($httpConfig);
		$this->startQuote = $this->endQuote = null;
	}

/**
 * Describe a model based on it's mapping
 *
 * @param Model $Model 
 * @return array Schema
 * @author David Kullmann
 */
	public function describe(Model $Model) {
		if (empty($this->_schema)) {
			$mapping = $this->getMapping();
			$this->_schema = $this->parseMapping($mapping);
		}
		
		if (empty($this->_schema[$Model->alias][$Model->primaryKey])) {
			$this->_schema[$Model->alias][$Model->primaryKey] = array('type' => 'string', 'length' => 255);
		} elseif (!empty($this->_schema[$Model->alias][$Model->primaryKey]['type'])) {
			if (empty($this->_schema[$Model->alias][$Model->primaryKey]['length'])) {
				if ($this->_schema[$Model->alias][$Model->primaryKey]['type'] === 'string') {
					$this->_schema[$Model->alias][$Model->primaryKey]['length'] = 255; 
				} elseif ($this->_schema[$Model->alias][$Model->primaryKey]['type'] === 'integer') {
					$this->_schema[$Model->alias][$Model->primaryKey]['length'] = 11;
				}
			}
		}
		
		return $this->_schema[$Model->alias];
	}

/**
 * List the types available for each index
 *
 * @return array Array of types - similar to tables in a DB
 * @author David Kullmann
 */
	public function listSources() {
		$sources = null;

		if ($this->_listSources) {
			$mapping = $this->getMapping();
			$sources = $this->parseMapping($mapping, true);
		}
		return $sources;
	}
	
	public function calculate(Model $Model, $func, $params = null) {
		
	}

/**
 * If this operation is atomic then store the model data to be created on "commit" later,
 * this allows for saving multiple models in one document through saveAll, saveMany, and saveAssociated.
 *
 * The first model to be saved in the transaction determines the type, and if more than one set of models (saveMany)
 * are being saved then they will be stored and saved in bulk
 * 
 * Otherwise simply index the model data given based on it's type and Id
 *
 * @param Model $Model 
 * @param array $fields 
 * @param array $values 
 * @return mixed boolean or array of model data
 * @author David Kullmann
 */
	public function create(Model $Model, $fields = array(), $values = array()) {

		$document = array($Model->alias => array_combine($fields, $values));

		if ($this->inTransaction()) {
			return $this->addToDocument($Model, $document);
		}

		$id = $this->_findKey($Model, $document);
		
		if (!$id) {
			throw new Exception('ElasticSource requires a primary key to index a document');
		}
		
		$results = $this->index($this->getType($Model), $id, $document);

		return $results;
	}

/**
 * Query ElasticSearch to retrieve records
 *
 * @param Model $Model 
 * @param array $queryData 
 * @return mixed boolean false on failure or array of records on success
 * @author David Kullmann
 */
	public function read(Model $Model, $queryData = array()) {

		$this->currentModel($Model);

		$query = $this->generateQuery($Model, $queryData);

		if (is_string($query)) {
			$api = $query;
			$query = null;
		} else {
			$api = $Model->findQueryType === 'count' ? '_count' : '_search';
		}

		$results = $this->get($this->getType($Model), $api, $query);


		if($Model->findQueryType === 'count') {
			return array(array($Model->alias => array('count' => $results)));
		}
		return $results;
	}
	
	public function update(Model $Model, $fields = array(), $values = array()) {
		return $this->create($Model, $fields, $values);
	}

/**
 * Delete a record
 *
 * @param Model $Model 
 * @param array $conditions 
 * @return boolean True on success
 * @author David Kullmann
 */
	public function delete(Model $Model, $conditions = null) {
		if (!empty($conditions)) {
			$record = $Model->find('first', $conditions);
		} else {
			$record = $Model->findById($Model->id);
		}
		
		$id = $record[$Model->alias][$Model->primaryKey];
		$type = $this->getType($Model);

		return $this->_delete($type, $id);
	}
	
	public function query($method, $params, Model $Model) {
		if (preg_match('/find(All)?By(.+)/', $method, $matches)) {
			$type = $matches[1] === 'All' ? 'all' : 'first';
			$conditions = array( strtolower($matches[2]) => $params );
			return $Model->find($type, compact('conditions'));
			
		}
		throw new Exception ('Cannot call method ' . $method . ' on ElasticSource');
	}
	
	public function index($type, $id, $document = array()) {
		return $this->put($type, $id, $document);
	}

/**
 * Bulk index a group of models
 *
 * @param string $type 
 * @param array $documents 
 * @return boolean true on success
 * @author David Kullmann
 */
	public function bulkIndex($type = null, $documents = array()) {
		
		$json = array();
		
		foreach ($documents as $id => $document) {
			$command = array('index' => array('_index' => $this->config['index'], '_type' => $type, '_id' => $id));
			$json[] = json_encode($command);
			$json[] = json_encode($document);
		}
		
		$json = implode("\n", $json) . "\n";

		return $this->post($type, '_bulk', $json, false);
	}

/**
 * Get/set the current model - called when requests are starting
 *
 * @param Model $Model 
 * @return Model the current model
 * @author David Kullmann
 */
	public function currentModel(Model &$Model) {
		if (is_object($Model)) {
			$this->currentModel = &$Model;
		}
		return $this->currentModel;
	}

/**
 * Check to see if we are in a transacion
 *
 * @param string $document 
 * @return void
 * @author David Kullmann
 */
	public function inTransaction() {
		if (empty($this->_transactionStarted)) {
			return false;
		}
		return true;
	}

/**
 * Begin a transaction - allows saving of multiple models pseudo-automically through _bulk API, but it's main
 * purpose is to allow saving associated model data and multiple models (saveMany/saveAssociated)
 *
 * @return true
 * @author David Kullmann
 */
	public function begin() {
		if (!$this->_transactionStarted) {
			$this->reset();
		}
		return $this->_transactionStarted = true;
	}

/**
 * Save all models we have been storing
 *
 * @return boolean true on success
 * @author David Kullmann
 */
	public function commit() {
		$documents = $this->allDocuments();

		$results = $this->bulkIndex($this->_type, $documents);

		$this->reset();
		return $results;
	}

/**
 * Cancel this save operation/transaction
 *
 * @return void
 * @author David Kullmann
 */
	public function rollback() {
		$this->reset();
	}

/**
 * Reset our transaction state, document, id, and type 
 *
 * @return void
 * @author David Kullmann
 */
	public function reset() {
		$this->_transactionStarted = false;
		$this->_document = array();
		$this->_type = null;
		$this->_id = null;
	}

/**
 * Store a document to save later, merge it with existing model information for that document
 *
 * Allows saveAssociated to work and allows storing multiple models in one document
 *
 * @param Model $Model 
 * @param array $document 
 * @return boolean true
 * @author David Kullmann
 */
	public function addToDocument(Model $Model, $document = array()) {
		$this->_setupTransaction($Model, $document);
		$this->_document[$this->_id] = Set::merge($this->_document[$this->_id], $document);
		return true;
	}

/**
 * Get the current document
 *
 * @return array Document data
 * @author David Kullmann
 */
	public function currentDocument() {
		return $this->_document[$this->_id];
	}

/**
 * Get all documents for this transaction
 *
 * @return array Array of documents keyed by their primary key
 * @author David Kullmann
 */
	public function allDocuments() {
		return $this->_document;
	}
	


/**
 * If we are in a new transaction, set _type and _id, if we are in an existing
 * transaction then start a new document and set _id
 *
 * @param Model $Model 
 * @param array $document 
 * @return void
 * @author David Kullmann
 */
	protected function _setupTransaction(Model $Model, $document = array()) {
		
		$id = $this->_findKey($Model, $document);
		$type = $this->getType($Model);
		
		if (empty($this->_type) && empty($this->_id) && $id) {
			$this->_type = $type;
			$this->_id   = $id;
			$this->_document[$id] = array();
		} elseif ($this->_type === $type && $id !== $this->_id && $id) {
			$this->_document[$id] = array();
			$this->_id = $id;
		}
		
		return true;
	}

/**
 * Generate an ElasticSearch query from Cake's ORM $queryData
 *
 * @param array $queryData 
 * @return array Array that can be converted to JSON for ElasticSearch
 * @author David Kullmann
 */
	public function generateQuery(Model $Model, $queryData = array()) {
		
		$queryKeys = array(
			'conditions' => 'filter',
			'limit' => 'size',
			'page' => 'from',
			'query' => 'query',
			'order' => 'sort',
			'fields' => 'fields',
			'facets' => 'facets'
		);
		
		$queryData['conditions'] = $this->parseConditions($Model, $queryData['conditions']);
		
		$queryData['conditions'] = $this->afterParseConditions($Model, $queryData['conditions']);

		if (is_string($queryData['conditions'])) {
			return $queryData['conditions'];
		}
		
		$queryData['order'] = $this->parseOrder($Model, $queryData);

		$query = array();
		
		if (empty($queryData['limit'])) {
			$queryData['limit'] = 10;
		}
		
		if($queryData['page'] === 1) {
			$queryData['page'] = 0;
		} else {
			$queryData['page'] = ($queryData['page'] - 1) * $queryData['limit'];
		}
		
		foreach ($queryKeys as $old => $new) {
			if(!empty($queryData[$old])){
				$query[$new] = $queryData[$old];
			}
		}
		
		$query['type'] = $this->parseQueryType($query);
		
		$query['query'] = empty($query['query']) ? array('match_all' => new Object()) : $query['query'];

		extract($query);
		
		if ($type !== 'query') {
			$query = array($type => compact('query', 'filter'));
		}


		$query = compact('query', 'size', 'sort', 'from', 'fields', 'facets');

		//$query = Set::filter($query);

		if ($Model->findQueryType === 'count') {
			return $query['query'];
		}
		//debug(json_encode($query));exit;
		return $query;
	}
	
	public function parseQueryType($query) {
		$type = 'filtered';
		if (!empty($query['type'])) {
			$type = $query['type'];
		}
		if (empty($query['filter'])) {
			if (empty($query['query'])) {
				$type = 'query';
			} else {
				$type = 'query_string';
			}
		}
		return $type;
	}

/**
 * Parse the 'conditions' key of a query from CakePHP's ORM
 *
 * @param array $conditions 
 * @return array Array of filters for ElasticSearch
 * @author David Kullmann
 */
	public function parseConditions(Model $Model, $conditions = array()) {

		$filters = array();
		if (!empty($conditions)) {
			foreach ($conditions as $key => $value) {
				$data = $this->_parseKey($Model, trim($key), $value);
				if (!empty($data)) {
					$filters[] = $data;
					$data = null;
				}
			}
		}

		return $filters;
	}
	
	public function parseOrder(Model $Model, $query = array()) {
		
		$results = array();
		
		$order = $query['order'];
		
		foreach ($order as $key => $value) {
			
			if ($key === 0 && empty($value)) {
				return false;
			}
			
			$direction = 'asc';
			
			if (is_string($value)) {
			} elseif (is_array($value)) {
				$field = key($value);
				$direction = current($value);
			}
			
			if (strpos($field, '.')) {
				list($alias, $field) = explode('.', $field);
			} else {
				$alias = $Model->alias;
			}
			
			if ($alias !== $Model->alias) {
				$aliasModel = ClassRegistry::init($alias);
				$type = $aliasModel->getColumnType($field);
			} else {
				$type = $Model->getColumnType($field);
			}
			
			switch ($type) {
				case 'geo_point':
					$results[] = array(
						'_geo_distance' => array(
							implode('.', array($alias, $field)) => array(
								'lat' => $query['latitude'],
								'lon' => $query['longitude']
							),
							'order' => strtolower($direction),
							'distance_type' => 'plane'
						)
					);
					break;
				default:
					$results[] = array($alias.'.'.$field => array('order' => strtolower($direction)));
			}
		}
		
		return $results;
	}

/**
 * Used to parse a key for ElasticSearch filters
 *
 * @param string $key 
 * @param mixed $value 
 * @return array ElasticSearch compatible filter
 * @author David Kullmann
 */
	protected function _parseKey(Model $Model, $key, $value) {
		if (is_numeric($key)) {
			if (empty($value)) {
				return false;
			} else {
				$key = key($value);
				$value = current($value);
			}
		}

		$filter = array();
		
		$operatorMatch = '/^(((' . implode(')|(', $this->_filterOps);
		$operatorMatch .= ')\\x20?)|<[>=]?(?![^>]+>)\\x20?|[>=!]{1,3}(?!<)\\x20?)/is';
		$bound = (strpos($key, '?') !== false || (is_array($value) && strpos($key, ':') !== false));

		if (strpos($key, ' ') === false) {
			$operator = '=';
		} else {
			list($key, $operator) = explode(' ', trim($key), 2);

			if (!preg_match($operatorMatch, trim($operator)) && strpos($operator, ' ') !== false) {
				$key = $key . ' ' . $operator;
				$split = strrpos($key, ' ');
				$operator = substr($key, $split);
				$key = substr($key, 0, $split);
			}
		}
		
		if (in_array($key, array('AND', 'OR', 'NOT'))) {
			$result = $this->parseConditions($Model, $value);
			if ($key === 'NOT') {
				if (count($result) === 1) {
					$result = current($result);
				} else {
					$result = array('and' => $result);
				}
			}
			return array(strtolower($key) => $result);
		}
		$type = $Model->getColumnType($key);
		if ($value === null) {
			$filter = $this->missing($key, $value);
		} else {
			switch ($type) {
				case 'float':
					$filter = $this->range($key, $operator, (float)$value);
					break;
				case 'integer':
					$value = (integer)$value;
				case 'date':
					$filter = $this->range($key, $operator, $value);
					break;				
				case 'multi_field':
				case 'string':
					$filter = $this->term($key, $operator, $value);
					 break;
				case 'geo_point':
					$filter = $this->geo($key, $operator, $value);
					break;
				case 'boolean':
					$filter = $this->term($key, $operator, $value);
					break;
				default:
					throw new Exception("Unable to process field of type '$type' for key '$key'");
			}
		}

		return $filter;
	}

/**
 * Perform this check after parseConditions has completed, since parseConditions is recursive
 * we have to perform this check in a separate method (or use a static variable or whatever, but,
 * I think this is cleaner)
 *
 * @param string $Model 
 * @param array $filters 
 * @return array
 * @author David Kullmann
 */	
	public function afterParseConditions(Model $Model, $filters = array()) {
		if (count($filters) > 1) {
			$filters = array('and' => $filters);
		} elseif (!empty($filters[0])) {
			$filters = $filters[0];
			if (!empty($filters['term']['id']) && count($filters['term']['id']) === 1) {
				return $filters['term']['id'][0];
			}
		}
		return $filters;
	}
	
	public function missing($key, $value) {
		return array('missing' => array('field' => $key));
	}
	
	public function term($key, $operator, $value) {
		$type = 'term';
		if (is_array($value) && count($value) > 1) {
			$type = 'terms';
		}
		return array($type => array($key => $value));
	}
	
	public function range($key, $operator, $value) {
		if ($operator === '=') {
			return $this->term($key, $operator, $value);
		}
		$rangeOperators = array(
			'>=' => 'gte',
			'>'  => 'gt',
			'<=' => 'lte',
			'<'  => 'lt'
		);
		$operator = $rangeOperators[$operator];
		return array('range' => array($key => array($operator => $value)));
	}
	
	public function geo($key, $operator, $value) {
		$return = array();
		if (is_array($value)) {
			$isBoundingBox = in_array(key($value), array('top_left', 'bottom_right'));
			if ($isBoundingBox) {
				$return = array(
					'geo_bounding_box' => array(
						$key => $value
					)
				);
			}
		} else {
			$return = array('geo_distance_range' => array(
				'lte' => $value,
				$key => array(
					'lat' => $this->currentModel->latitude,
					'lon' => $this->currentModel->longitude
				),
				'unit' => 'miles',
				'distance_type' => 'plane'
			));
		}
		return $return;
	}

/**
 * Find the key for this document
 *
 * @param Model $Model 
 * @param array $document 
 * @return mixed Boolean false if no key is present, otherwise the key (string/int)
 * @author David Kullmann
 */
	protected function _findKey(Model $Model, $document = array()) {
		if (method_exists($Model, 'key')) {
			return $Model->key($document);
		}
		return !empty($document[$Model->alias][$Model->primaryKey]) ? $document[$Model->alias][$Model->primaryKey] : false;
	}

/**
 * Get the entire index mapping
 *
 * @return array ES Mapping
 * @author David Kullmann
 */
	public function getMapping() {
		return $this->get(null, '_mapping');
	}

/**
 * Parse an entire index mapping to create the schema for this datasource
 *
 * @param array $mapping 
 * @return array CakePHP schema
 * @author David Kullmann
 */
	public function parseMapping($mapping = array(), $sourcesOnly = false) {
		$schema = array();
		if (!empty($mapping)) {
			foreach ($mapping as $index => $types) {
				if($sourcesOnly) {
					$schema = array_merge($schema, array_keys($types));
				} else {
					foreach ($types as $type => $models) {
						foreach ($models['properties'] as $alias => $properties) {
							$schema[$alias] = $properties['properties'];
						}
					}
				}
			}
		}
		return $schema;
	}

/**
 * Get the useType if its set, otherwise use the table name
 *
 * @param Model $Model 
 * @return void
 * @author David Kullmann
 */
	public function getType(Model $Model) {
		return !empty($Model->useType) ? $Model->useType : $Model->useTable;
	}
	
	public function createIndex($index, $alias = false) {
		$type = 'put';
		$api = $index;
		$body = null;

		$tmp = $this->config['index'];
		$this->config['index'] = null;
		
		if ($alias) {
			$type = 'post';
			$api = '_aliases';
			$actions = array();
			$actions[] = array('add' => compact('index', 'alias'));
			$body = compact('actions');
		}

		try {
			$return = $this->{$type}(null, $api, $body);
		} catch (Exception $e) {
			$message = $e->getMessage();
			if (preg_match('/IndexAlreadyExistsException/', $message)) {
				throw new Exception("ElasticSearch index '$index' already exists");
			} else {
				throw $e;
			}
		}
	
		$this->config['index'] = $tmp;
	
		return $return;
	}
	
	public function dropIndex($index) {
		$tmp = $this->config['index'];
		$this->config['index'] = $index;
		$results = $this->_delete();
		$this->config['index'] = $tmp;
		return $results;
	}

/**
 * Check to see if a mapping exists
 *
 * @param Model $Model 
 * @return boolean true if it exists
 * @author David Kullmann
 */
	public function checkMapping(Model $Model) {
		
		$type = $this->getType($Model);
		
		$api = '_mapping';

		$mappings = $this->get(null, $api);
		
		return !empty($mappings[$this->config['index']][$type]);
	}

/**
 * Map a model based on it's description from MySQL (or your own)
 *
 * @param Model $Model 
 * @param array $description 
 * @return boolean true on success
 * @author David Kullmann
 */
	public function mapModel(Model $Model, $description = array()) {

		if (empty($description[$Model->alias])) {
			$tmp = $description;
			unset($description);
			$description = array($Model->alias => $tmp);
		}

		$properties = $this->_parseDescription($Model, $description);
		
		$type = $this->getType($Model);
		
		$mapping = array($type => compact('properties'));

		$result = $this->put($type, '_mapping', $mapping);

		return $result;
	}

/**
 * Delete a mapping
 *
 * @param Model $Model 
 * @return boolean true on success
 * @author David Kullmann
 */
	public function dropMapping(Model $Model) {
		return $this->_delete($this->getType($Model));
	}
	

/**
 * Call HttpSocket methods
 *
 * @param string $method 
 * @param array $arguments 
 * @return mixed Results from HttpSocket calls parsed by _parseResponse and _filterResults
 * @see ElasticSource::_parseResponse()
 * @see ElasticSource::_filterResults()
 * @author David Kullmann
 */
	public function __call($method, $arguments) {
		
		$method = $method === '_delete' ? 'delete' : $method;
		
		if (method_exists($this->Http, $method)) {
			
			if (isset($arguments[3]) && $arguments[3] === false) {
				$encode = false;
			} else {
				$encode = true;
			}
			
			$type    = !empty($arguments[0]) ? $arguments[0] : null;
			$api     = !empty($arguments[1]) ? $arguments[1] : null;
			
			if (!empty($arguments[2])) {
				$body = $encode ? json_encode($arguments[2]) : $arguments[2];
			} else {
				$body = null;
			}

			$path = array_filter(array($this->config['index'], $type, $api));

			$path = '/' . implode('/', $path);
			
			$uri = $this->_uri(compact('path'));
			
			$response = array();

			switch ($method) {
				case 'get':
					$response = call_user_func_array(array(&$this->Http, $method), array($uri, array(), compact('body')));
					break;
				default:
					$response = call_user_func_array(array(&$this->Http, $method), array($uri, $body));
			}

			$results = $this->_parseResponse($response);
			
			$this->logQuery($method, $uri, $body, $results);

			$results = $this->_filterResults($results);
			
			return $results;
		} else {
			throw new Exception("Method $method does not exist on ElasticSource");
		}
	}

/**
 * Get the URI for a request
 *
 * @param array $config HttpSocket $config style array 
 * @return array Array compatible with HttpSocket $uri for get/post/put/delete 
 * @author David Kullmann
 */
	protected function _uri($config) {
		$config = Set::merge($this->Http->config, $config);
		unset($config['request']);
		return $config;
	}

/**
 * Parse the response from ElasticSearch, throwing errors if necessary
 *
 * @param CakeResponse $response 
 * @return mixed boolean true or false, or body of request as array
 * @author David Kullmann
 */
	protected function _parseResponse($response) {

		if (empty($response->body)) {
			throw new Exception('Missing response');
		}
		
		$body = json_decode($response['body']);
		
		if (!empty($body->items)) {
			foreach ($body->items as $item) {
				if (!empty($item->index->error)) {
					throw new Exception('ElasticSearch Indexing Error ' . $item->index->error);
				}
			}
		}
		
		if (!empty($body->error)) {
			return $this->_throwError($body);
		}

		if (!empty($body->ok) && $body->ok == true) {
			return true;
		}
		return Set::reverse($body);
	}

/**
 * Filter results from a call, parsing out the records
 *
 * @param array $results 
 * @return array Array of results
 * @author David Kullmann
 */
	protected function _filterResults($results = array()) {
		if (!empty($this->currentModel)) {
			if ($this->currentModel->findQueryType === 'count') {
				return empty($results['count']) ? $results : $results['count'];
			}
		}
		if (!empty($results['facets'])) {
			if (!isset($this->currentModel->_facets)) {
				$this->currentModel->_facets = array();
			}
			foreach ($results['facets'] as $facet => $data) {
				$this->currentModel->_facets[$facet] = $data;
			}
		}
		if (!empty($results['hits'])) {
			foreach($results['hits']['hits'] as &$result) {
				$tmp = isset($result['_source']) ? $result['_source'] : array();
				if (!empty($result['fields'])) {
					foreach ($result['fields'] as $field => $value) {
						if (strpos($field, '.') && strpos($field, 'doc') !== 0) {
							list($alias, $field) = explode('.', $field);
							$tmp[$alias][$field] = $value;
						} else {
							$tmp[0][$field] = $value;
						}
					}
				}
				if (empty($tmp[$this->currentModel->alias][$this->currentModel->primaryKey])) {
					$tmp[$this->currentModel->alias][$this->currentModel->primaryKey] = $result['_id'];
				}
				
				$result = $tmp;
			}
			return $results['hits']['hits'];
		}
		if (!empty($results['_id'])) {
			$model = $results['_source'];
			if (empty($model[$this->currentModel->alias][$this->currentModel->primaryKey])) {
				$model[$this->currentModel->alias][$this->currentModel->primaryKey] = $results['_id'];
			}
			return array($model);
		}
		return $results;
	}

/**
 * Recursive method to map a SQL-like model description into a ElasticSearch one
 *
 * @param array $description 
 * @return array Array representing ES Mapping
 * @author David Kullmann
 */
	protected function _parseDescription(Model $Model, $description = array()) {
		
		$properties = array();
		
		foreach ($description as $field => $info) {
			if (is_array($info)) {

				$current = current($info);

				if (is_array($current)) {
					$properties[$field] = array(
						'properties' => $this->_parseDescription($Model, $info),
						'type' => 'object'
					);
				} else {
					if (method_exists($Model, '_esFieldMapping')) {
						$override = $Model->_esFieldMapping($field);
					} else {
						$override = false;
					}
					if ($override) {
						$properties[$field] = $override;
					} else {
						foreach ($info as $attr => $val) {
							$val = $this->_convertAttributes($attr, $val);
							if ($val) {
								$properties[$field][$attr] = $val;
								if ($val === 'date') {
									$properties[$field]['format'] = 'yyyy-MM-dd HH:mm:ss';
								}
							}
						}
						
					}
				}
			}
		}
		
	 	return $properties;
		
	}

/**
 * Convert MySQL or CakePHP ORM field attributes into ElasticSearch compatible attributes
 *
 * @param string $attr 
 * @param string $val 
 * @return string ES compatible attribute
 * @author David Kullmann
 */
	protected function _convertAttributes($attr, $val) {

		$notUsed = array('null', 'collate', 'length', 'default', 'key', 'charset');

		if (in_array($attr, $notUsed)) {
			return false;
		}
		
		if ($attr === 'type' && $val === 'datetime') {
			$val = 'date';
		}

		return $val;
	}

/**
 * Log a new query if debug is on
 *
 * @param string $method 
 * @param string $uri 
 * @param string $body 
 * @param string $results 
 * @return void
 * @author David Kullmann
 */
	public function logQuery($method, $uri, $body, $results = array()) {
		if (Configure::read('debug')) {
			$query = strtoupper($method) . ': ' . $this->Http->url($uri) . " $body";
			$took = !empty($results['took']) ? $results['took'] : 0;
			$affected = !empty($results['hits']['total']) ? $results['hits']['total'] : 0;
			$numRows = !empty($results['hits']['hits']) ? count($results['hits']['hits']) : 0;
			$log = compact('query', 'affected', 'numRows', 'took');
			
			return $this->_addLog($log);
		}
		return true;
	}

/**
 * Get the query log - support for DebugKit Toolbar
 *
 * @return void
 * @author David Kullmann
 */
	public function getLog() {
		return $this->_queryLog;
	}

/**
 * Added an item to the query log
 *
 * @param string $data 
 * @return void
 * @author David Kullmann
 */
	protected function _addLog($data = array()) {
		$count = $this->_queryLog['count'];
		foreach ($data as $key => $value) {
			$this->_queryLog['log'][$count][$key] = $value;
			if ($key === 'took') {
				$this->_queryLog['time'] += $value;
			}
		}
		$this->_queryLog['count']++;
		return $this->_queryLog['log'][$count];
	}

/**
 * Throw the right error
 *
 * @param string $info 
 * @return void
 * @author David Kullmann
 */
	protected function _throwError($info) {
		switch($info->status) {
			case '404':
				throw new MissingIndexException(array('class' => $this->config['index']));
			default:
				throw new Exception("ElasticSearch Error: " . $info->error . ' Status: ' . $info->status);
		}
	}
}
?>