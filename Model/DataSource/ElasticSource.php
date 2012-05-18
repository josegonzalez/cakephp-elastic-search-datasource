<?php

App::uses('HttpSocket', 'Network/Http');

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
		return $this->_schema[$Model->alias];
	}

	public function listSources() {
		
	}
	
	public function calculate(Model $Model, $func, $params) {
		
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
		
		$results = $this->index($Model->useTable, $id, $document);

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
		
		$query = $this->generateQuery($queryData);

		$results = $this->get($Model->useTable, '_search', $query);
		
		return $results;
	}
	
	public function update(Model $Model, $fields = array(), $values = array()) {
		// Not yet implemented
		throw new Exception('Update not yet implemented by ElasticSource');
	}
	
	public function delete(Model $Model, $conditions = null) {
		// Not yet implemented
		throw new Exception('Delete not yet implemented by ElasticSource');	
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
		$type = $Model->useTable;
		
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
	public function generateQuery($queryData = array()) {
		
		$queryKeys = array(
			'conditions' => 'filter',
			'limit' => 'size',
			'page' => 'from',
			'query' => 'query',
			'order' => 'sort',
		);
		
		$queryData['conditions'] = $this->parseConditions($queryData['conditions']);
		
		$query = array();
		
		foreach ($queryKeys as $old => $new) {
			$query[$new] = empty($queryData[$old]) ? null : $queryData[$old];
		}
		
		
		$query['query'] = empty($query['query']) ? array('match_all' => new Object()) : $query['query'];
		
		$query['type'] = empty($queryData['type']) ? 'filtered' : $queryData['type'];
		
		extract($query);
		
		$query = array($type => compact('query', 'filter'));

		$query = compact('query', 'size', 'sort', 'from');
		
		$query = Set::filter($query);
				
		return $query;
	}

/**
 * Parse the 'conditions' key of a query from CakePHP's ORM
 *
 * @param array $conditions 
 * @return array Array of filters for ElasticSearch
 * @author David Kullmann
 */
	public function parseConditions($conditions = array()) {

		$filters = array();
		
		foreach ($conditions as $key => $value) {
			$data = $this->_parseKey(trim($key), $value);
			if (!empty($data)) {
				$filters[] = $data;
				$data = null;
			}
		}
		return array('and' => $filters);
	}

/**
 * Used to parse a key for ElasticSearch filters
 *
 * @param string $key 
 * @param mixed $value 
 * @return array ElasticSearch compatible filter
 * @author David Kullmann
 */
	protected function _parseKey($key, $value) {
		$filter = array('term' => array($key => $value));
		return $filter;
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
	public function parseMapping($mapping = array()) {
		$schema = array();
		if (!empty($mapping)) {
			foreach ($mapping as $index => $types) {
				foreach ($types as $type => $models) {
					foreach ($models['properties'] as $alias => $properties) {
						$schema[$alias] = $properties['properties'];
					}
				}
			}
		}
		return $schema;
	}

/**
 * Check to see if a mapping exists
 *
 * @param Model $Model 
 * @return boolean true if it exists
 * @author David Kullmann
 */
	public function checkMapping(Model $Model) {
		
		$type = $Model->useTable;
		
		$api = '_mapping';
		
		try {
			$results = $this->get($type, $api);
		} catch (Exception $e) {
			return false;
		}

		return !empty($results);
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
		
		$properties = $this->_parseDescription($description);
		
		$type = $Model->useTable;
		
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
		return $this->_delete($Model->useTable);
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

		if (!empty($body->error)) {
			throw new Exception("ElasticSearch Error: " . $body->error . ' Status: ' . $body->status);
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
		if (!empty($results['hits'])) {
			return $results['hits']['hits'];
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
	protected function _parseDescription($description = array()) {
		
		$properties = array();
		
		foreach ($description as $field => $info) {
			if (is_array($info)) {

				$current = current($info);

				if (is_array($current)) {
					$properties[$field] = array(
						'properties' => $this->_parseDescription($info),
						'type' => 'object'
					);
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
}
?>