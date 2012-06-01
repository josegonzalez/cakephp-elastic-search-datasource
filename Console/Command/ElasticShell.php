<?php
App::uses('ConnectionManager', 'Model');
class ElasticShell extends Shell {
	
	public function getOptionParser() {
		$parser = parent::getOptionParser();
		return $parser->description('ElasticSearch Plugin Console Commands. Map and index data')
			->addSubcommand('mapping', array('help' => 'Map a model to ElasticSearch'))
			->addSubcommand('list_sources', array('help' => 'Display output from listSources'))
			->addSubcommand('index', array('help' => 'Index a model into ElasticSearch'))
				->addOption('action', array('help' => 'Action to do for mapping ("drop" or "create")','default' => 'create', 'short' => 'a'))
				->addOption('model', array('help' => 'Model to use','short' => 'm'))
				->addOption('limit', array('help' => 'Limit for indexing','short' => 'l', 'default' => 100))
				->addOption('page', array('help' => 'Page to start indexing on','short' => 'p', 'default' => 1))
				->addOption('reset', array('help' => 'Also reset the reset the mappings','short' => 'r', 'default' => 0));
	}

	public function mapping() {
		extract($this->params);
		
		$this->Model = $this->_getModel($model);
		
		$ds = ConnectionManager::getDataSource('index');
		$mapped = $ds->checkMapping($this->Model);

		if ($action === 'create') {
			if (!$mapped) {
				$this->out("<info>Mapping " . $this->Model->alias . '</info>');
				
				if (method_exists($this->Model, 'elasticMapping')) {
					$mapping = $this->Model->elasticMapping();
				} else {
					$dboDS = ConnectionManager::getDataSource($this->Model->useDbConfig);
					$mapping = $dboDS->describe($this->Model);
				}
				
				$ds->mapModel($this->Model, $mapping);
			} else {
				$this->out($this->Model->alias . ' already mapped');
			}
		} elseif ($action === 'drop') {
			if (!$mapped) {
				$this->out('Mapping does not exist yet');
			} else {
				$return = $ds->dropMapping($this->Model);
				
				if ($return) {
					$this->out("<info>Mapping for " . $this->Model->alias . ' has been dropped</info>');
				}
				
			}
		}
	}
	
	public function list_sources() {

		$ds = ConnectionManager::getDataSource('index');

		$sources = $ds->listSources();

		if (!empty($sources)) {
			foreach ($sources as $source) {
				$this->out('Found: ' . $source);
			}
		} else {
			$this->out('<warning>listSources did not return any sources</warning> This could be OK');
		}
	}
	
	protected function _getModel($modelName) {
		return ClassRegistry::init($modelName);
	}
	
	public function index() {
		
		extract($this->params);
		
		$this->Model = $this->_getModel($model);
		
		list($alias, $field) = $this->Model->getModificationField();

		$db = $this->Model->useDbConfig;

		$this->Model->setDataSource('index');
		$date = $this->Model->lastSync();
		$this->Model->setDataSource($db);
		
		$conditions = array($this->Model->alias.'.'.$field . ' >=' => $date);
		$this->out('Retrieving data from mysql starting on ' . $date);
		
		$order = array($this->Model->alias.'.'.$field => 'ASC');
		$contain = false;
		
		$records = array();
		
		do {
			if(!empty($records)) {
				$record = array_pop($records);
				$newDate = $record[$alias][$field];
				if($newDate === $date) {
					$page++;
				} else {
					$page = 1;
				}
				$date = $newDate;
				$conditions = array($this->Model->alias.'.'.$field . ' >=' => $newDate);
			}
			
			$records = $this->Model->find('all', compact('conditions', 'limit', 'page', 'order'));

			if (!empty($records)) {
				
				$this->Model->create();
				$this->Model->setDataSource('index');
				$results = $this->Model->saveAll($records, array('deep' => true));

				if ($results) {
					$count = count($records);
					$this->out("Saved $count records starting on $date (page: $page - limit: $limit)");
				} else {
					$this->out("Unable to save records (limit: $limit - page: $page)");
				}

			}
			
			$this->Model->setDataSource('default');
			
		} while (!empty($records));

	}

/**
 * If you save your document and you get a mapping error this might help you find it.
 *
 * @return void
 * @author David Kullmann
 */
	public function find_mapping_errors() {
		
		extract($this->params);
		
		$errors = false;
		
		if ($reset) {
			$this->params['action'] = 'drop';
			$this->mapping();
			$this->params['action'] = 'create';
			$this->mapping();
		}
		
		$this->Model = $this->_getModel($model);

		$record = $this->Model->find('first');
		
		$this->Model->setDataSource('index');
				
		try {
			$results = $this->Model->saveAll($record);
		} catch (Exception $e) {
			$errors = true;
			$message = $e->getMessage();
			$this->out($message);
			
			$testField = array();
			
			foreach($record as $alias => $data) {
				foreach ($data as $key => $value) {
					$failed = false;
					$testField = compact('key', 'value');
					unset($record[$alias][$key]);
					try {
						$results = $this->Model->saveAll($record);
					} catch (Exception $e) {
						$failed = true;
						$message = $e->getMessage();
					}
					if (!$failed) {
						$this->out('Your save worked after removing ' . $testField['key'] . ' with value ' . $testField['value'] );
						exit;
					} else {
						$this->out('<error>Save failed without key ' . $alias .'.'. $testField['key'] . "</error>\n" . $message);
					}
				}
			}
		}
		
		if (!$errors) {
			$this->out('No mapping errors found');	
		}
	}
	
}