<?php

App::uses('ConnectionManager', 'Model');
App::uses('ClassRegistry', 'Utility');

class ElasticShell extends AppShell {

/**
 * Used to track time on indexing
 *
 * @var string
 */
	public $timers = array();

/**
 * Start the timer for a task
 *
 * @param string $taskId
 * @return void
 * @author David Kullmann
 */
	public function _startTimer($taskId) {
		$this->timers[$taskId] = microtime(true);
	}

/**
 * End a timer for a task and print the output if --verbose
 *
 * @param string $taskId
 * @return $float the time this task took
 * @author David Kullmann
 */
	public function _endTimer($taskId) {
		$time = microtime(true) - $this->timers[$taskId];
		$this->out(sprintf("\tTask '%s' completed in %.2f seconds", $taskId, $time), 1, Shell::VERBOSE);
		return $time;
	}

/**
 * Option parser for this shell
 *
 * @return object OptionParser
 * @author David Kullmann
 */
	public function getOptionParser() {
		return parent::getOptionParser()
			->description('ElasticSearch Plugin Console Commands. Map and index data')
				->addSubcommand('create_index', array('help' => 'Create or alias an index', 'parser' => $this->getCreateIndexOptions()))
				->addSubcommand('mapping', array('help' => 'Map a model to ElasticSearch', 'parser' => $this->getMappingOptions()))
				->addSubcommand('index', array('help' => 'Index a model into ElasticSearch', 'parser' => $this->getIndexOptions()))
				->addSubcommand('copy_index', array('help' => 'Hot copies all data from one index to a new one', 'parser' => $this->getCopyIndexOptions()))
				->addSubcommand('list_sources', array('help' => 'Display output from listSources'));
	}

/**
 * Options for creating/dropping indexes
 *
 * @return object OptionParser
 * @author David Kullmann
 */
	public function getCreateIndexOptions() {
		return parent::getOptionParser()
			->addArgument('index', array('help' => 'The index you are creating', 'required' => true))
			->addOption('alias', array('help' => 'Instead of creating a new index, alias "index" to this value', 'default' => false, 'short' => 'a'))
			->addOption('drop', array('help' => 'Instead of creating a new index, drop "index"', 'default' => false, 'short' => 'd', 'boolean' => true))
			->addOption('connection', array(
				'short' => 'c',
				'help' => 'Name of datasource connection to be used as defined in database.php',
				'default' => 'index'
			));
	}

/**
 * For mapping a type
 *
 * @return object OptionParser
 * @author David Kullmann
 */
	public function getMappingOptions() {
		return parent::getOptionParser()
			->addOption('action', array('help' => 'Action to do for mapping (drop, create, update, check)', 'default' => 'create', 'short' => 'a'))
			->addArgument('model', array('help' => 'Model to use', 'required' => true))
			->addOption('connection', array(
				'short' => 'c',
				'help' => 'Name of datasource connection to be used as defined in database.php',
				'default' => 'index'
			));
	}

/**
 * For indexing records
 *
 * @return object OptionParser
 * @author David Kullmann
 */
	public function getIndexOptions() {
		return parent::getOptionParser()
			->addArgument('model', array('help' => 'Model to use', 'required' => true))
			->addOption('db_config', array('help' => 'DB config to use to get the schema', 'default' => 'default', 'short' => 'd'))
			->addOption('extra', array('help' => 'Extra param for you to use, useful when overriding IndexableBehavior::lastSync()/syncConditions()','short' => 'e'))
			->addOption('since', array('help' => 'Index records created after the given date','short' => 's', 'default' => null))
			->addOption('limit', array('help' => 'Limit for indexing','short' => 'l', 'default' => 100))
			->addOption('page', array('help' => 'Page to start indexing on','short' => 'p', 'default' => 1))
			->addOption('fast', array('help' => 'Fast index (dont use saveAll)','short' => 'f', 'default' => false))
			->addOption('reset', array('help' => 'Also reset the mappings','short' => 'r', 'default' => 0));
	}

/**
 * For indexing records
 *
 * @return object OptionParser
 * @author David Kullmann
 */
	public function getCopyIndexOptions() {
		return parent::getOptionParser()
			->addArgument('index', array('help' => 'The index you are creating', 'required' => true))
			->addArgument('model', array('help' => 'Model to use (Used for mappinps)', 'required' => true))
			->addOption('connection', array(
				'short' => 'c',
				'help' => 'Name of datasource connection to be used as defined in database.php',
				'default' => 'index'
			));
	}

/**
 * Create an index
 *
 * @return void
 * @author David Kullmann
 */
	public function create_index() {
		extract($this->params);

		$index = $this->args[0];

		$ds = ConnectionManager::getDataSource($this->params['connection']);

		$action = 'created';
		$alias = empty($this->params['alias']) ? false : $this->params['alias'];
		try {
			if (!empty($this->params['drop'])) {
				$action = 'dropped';
				$result = $ds->dropIndex($index);
			} else {
				$result = $ds->createIndex($index, $alias);
			}
		} catch (Exception $e) {
			$message = $e->getMessage();
			$this->out("<error>$message</error>");
			return false;
		}

		if ($result) {
			if ($alias) {
				$this->out("Successfully aliased $alias to $index");
			} else {
				$this->out("Successfully $action $index");
			}

		}

	}

/**
 * Create a mapping for an ES type
 *
 * @return void
 * @author David Kullmann
 */
	public function mapping() {
		extract($this->params);

		$model = $this->args[0];

		$this->Model = $this->_getModel($model);

		if (get_class($this->Model) === 'AppModel') {
			$this->out("<error>Couldn't load model $model</warning>");
			return false;
		}

		$ds = ConnectionManager::getDataSource($this->params['connection']);
		$mapped = $ds->checkMapping($this->Model);

		if ($action === 'create' || $action === 'update') {
			if (!$mapped || $action === 'update') {
				$this->out("<info>Mapping " . $this->Model->alias . '</info>');

				if (method_exists($this->Model, 'elasticMapping')) {
					$mapping = $this->Model->elasticMapping();
				} else {
					$mapping = $ds->describe($this->Model);
				}

				if (!empty($mapping)) {
					$ds->mapModel($this->Model, $mapping);
				} else {
					$this->out("Unable to find mapping for $model");
				}

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
		} elseif ($action === 'check') {
			$exists = $mapped ? '<info>exists</info>' : '<warning>does not exist</warning>';
			$this->out("Mapping for " . $this->Model->alias . ' ' . $exists);
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

		$model = $this->args[0];

		$this->Model = $this->_getModel($model);

		if (!$this->Model->Behaviors->attached('Indexable')) {
			$this->Model->Behaviors->load('Elastic.Indexable');
		}

		list($alias, $field) = $this->Model->getModificationField();

		if (!empty($this->params['since'])) {
			$date = date('Y-m-d H:i:s', strtotime($this->params['since']));
		} else {
			$this->Model->setDataSource('index');
			$date = $this->Model->lastSync($this->params);
			$this->Model->setDataSource($db_config);
		}

		$conditions = $this->Model->syncConditions($field, $date, $this->params);

		$this->out('Retrieving data from mysql starting on ' . $date);

		$order = array($this->Model->alias.'.'.$field => 'ASC');
		$contain = false;

		$records = array();

		$tasks = array(
			'mysql' => 'Retrieving MySQL records',
			'saving' => 'Saving to ElasticSearch'
		);

		// Incase you have a zillion records we don't use pagination because of
		// issues w/how MySQL does pagination. This will paginate properly even
		// if many models have the same value in the modification field
		do {
			if (!empty($records)) {
				$record = array_pop($records);
				$newDate = $record[$alias][$field];
				if($newDate === $date) {
					$page++;
				} else {
					$page = 1;
				}
				$date = $newDate;
				$conditions = $this->Model->syncConditions($field, $date, $this->params);
			}

			$this->_startTimer($tasks['mysql']);
			$records = $this->Model->find('all', compact('conditions', 'limit', 'page', 'order'));
			$this->_endTimer($tasks['mysql']);

			if (!empty($records)) {
				$this->Model->create();
				$this->Model->setDataSource('index');
				$this->_startTimer($tasks['saving']);
				if ($fast) {
					try {
						$results = $this->Model->index($records);
					} catch (Exception $e) {
						$this->out("Error: ". $e->getMessage());
					}
				} else {
					$results = $this->Model->saveAll($records, array('deep' => true, 'validate' => false));
				}
				$this->_endTimer($tasks['saving']);

				if ($results) {
					$count = count($records);
					$this->out("Saved $count records starting on $date (page: $page - limit: $limit)");
				} else {
					$this->out("Unable to save records (limit: $limit - page: $page)");
				}
			}

			$this->Model->setDataSource($db_config);
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
						return false;
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

/**
 * Copies all data from one index/type to another
 *
 * @return void
 **/
	public function copy_index() {
		$this->Model = $this->_getModel($this->args[1]);
		$ds = ConnectionManager::getDataSource($this->params['connection']);
		$cursor = $this->Model->scan(200, '5m');
		$ds->reindex($cursor, array('toIndex' => $this->args[0], 'toType' => $ds->getType($this->Model)));
		$this->out('<success>Data was reindex</success>');
	}

	public function _welcome() {

	}

}
