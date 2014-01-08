<?php
App::uses('AppModel', 'Model');

class ElasticAppModel extends AppModel {

	/**
	 * Name of database configuration resource to use for storing data in Elastic Search
	 *
	 * @var string
	 * @package
	 * */
	public $useIndexConfig = 'index';

	/**
	 *
	 */
	public $insertID;

	/**
	 * Check if DataSource currently is ElasticSource
	 *
	 * @return boolean
	 */
	public function isElastic() {
		return $this->getDataSource() instanceof ElasticSource;
	}

	/**
	 * Dynamically set the schema to the properties the corresponding data source is expecting
	 * when using the elastic search data source it will return the elastic mapping
	 *
	 * @param mixed $field (optional)
	 * @return array
	 */
	public function schema($field = false) {
		if ($this->isElastic()) {
			if (!empty($this->_schema) && !isset($this->_oldSchema)) {
				$this->_oldSchema = $this->_schema;
			}
			$schema = $this->_schema = $this->elasticMapping();
			if ($field && is_string($field)) {
				return Hash::get($schema, $field);
			}
			return $schema;
		}
		if (!empty($this->_oldSchema)) {
			$this->_schema = $this->_oldSchema;
			unset($this->_oldSchema);
		}
		// Failsafe to prevent schema mix-ups
		if (method_exists($this, 'elasticMapping') && $this->_schema === $this->elasticMapping()) {
			$this->_schema = null;
		}
		return parent::schema($field);
	}

	/**
	 * Is this field in this schema?
	 *
	 * @param string $name
	 * @param boolean $checkVirtual (optional)
	 * @return boolean
	 */
	public function hasField($name, $checkVirtual = false) {
		return (in_array($name, array_keys($this->_mapping)));
	}

	/**
	 * get the last insertID property
	 *
	 * @return mixed $insertID
	 */
	public function getInsertID() {
		return $this->insertID;
	}

	/**
	 * beforeSave callback
	 *
	 * @param array $options (optional)
	 * @return boolean
	 */
	public function beforeSave($options = array()) {
		// note, this is bypassed!! ---------- <<< ---------
		return true;
		if (!isset($this->data[$this->alias]['id']) || !isset($this->id) ) {
			$this->insertID = $this->noSqlID();
			$this->data[$this->alias]['id'] = $this->insertID;
		}
		$this->data[$this->alias]['id'] = (isset($this->id) ? $this->id : $this->data[$this->alias]['id']);
	}

	/**
	 * return the mapping schema for this model
	 *
	 * @return array $mapping / $schema
	 */
	public function elasticMapping() {
		return $this->_mapping;
	}

	/**
	 * Does this ID / record exist?
	 *
	 * @param mixed $id (optional)
	 * @return boolean
	 */
	public function exists($id = null) {
		$id = ($id ? $id : $this->id);
		$conditions = array('id' => $id);
		return $this->find('count', array('conditions' => $conditions )) == 1 ? true : false;
	}

	/**
	 * Counter cache should not happen when you are in ElasticSearch mode
	 *
	 * Updates the counter cache of belongsTo associations after a save or delete operation
	 *
	 *  'counterScope' defined get updated
	 *
	 * @param array   $keys    (optional) Optional foreign key data, defaults to the information $this->data
	 * @param boolean $created (optional) True if a new record was created, otherwise only associations with
	 * @return void
	 */
	public function updateCounterCache($keys = array(), $created = false) {
		if ($this->isElastic()) {
			return;
		}
		return parent::updateCounterCache($keys, $created);
	}

	/**
	 * Cascades model deletes through associated hasMany and hasOne child records.
	 *
	 * @param string  $id      ID of record that was deleted
	 * @param boolean $cascade Set to true to delete records that depend on this record
	 * @return void
	 */
	protected function _deleteDependent($id, $cascade) {
		if ($this->isElastic()) {
			return;
		}
		return parent::_deleteDependent($id, $cascade);
	}

	/**
	 * Cascades model deletes through HABTM join keys.
	 *
	 * @param int     $id ID of record that was deleted
	 * @return void
	 */
	protected function _deleteLinks($id) {
		if ($this->isElastic()) {
			return false;
		}
		parent::_deleteLinks($id);
	}

	/**
	 * Switches internal datasource config to elastic search
	 *
	 * @return void
	 */
	public function switchToElastic() {
		if ($this->isMysql()) {
			$this->_oldConfig = $this->useDbConfig;
		}
		$this->setDataSource($this->useIndexConfig);
	}

	/**
	 * Switches the model back from elastic search datasource to database
	 *
	 * @return void
	 * */
	public function switchToDatabase() {
		if ($this->isElastic()) {
			$config = (empty($this->_oldConfig)) ? 'default' : $this->_oldConfig;
			$this->_oldConfig = null;
			$this->setDataSource($config);
		}
	}
}
