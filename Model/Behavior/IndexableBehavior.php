<?php
Class IndexableBehavior extends ModelBehavior {

/**
 * Support for a distance find
 *
 * @var string
 */
	public $mapMethods = array(
		'/^_findGeo$/' => '_findGeo'
	);

/**
 * If performing a _geo_distance query and you want to capture the distance you'll need this field
 *
 * @var string
 */
	public $distanceField = null;

/**
 * Support for having a geo_point field - just one at the moment
 *
 * @var array
 */
	public $_defaults = array(
		'geoFields' => array(
			'latitude' => null,
			'longitude' => null,
			'location' => null,
			'alias' => null
		),
		'modificationField' => 'modified'
	);

/**
 * Setup the model
 *
 * @param Model $Model
 * @param array $settings
 * @return void
 * @author David Kullmann
 */
	public function setup(Model $Model, $settings = array()) {
		parent::setup($Model, $settings);
		$Model->findMethods['geo'] = true;
		$this->settings[$Model->alias] = Set::merge($this->_defaults, $settings);
	}

/**
 * Geo-distance find - specialized for ElasticSource
 *
 * @param Model $Model
 * @param string $method
 * @param string $state
 * @param array $query
 * @param array $results
 * @return void
 * @author David Kullmann
 */
	public function _findGeo(Model $Model, $method, $state, $query, $results = array()) {
		if ($state === 'before') {
			$query = $this->parseGeoQuery($Model, $query);
			return $query;
		} elseif ($state === 'after') {
			$geoField = $this->settings[$Model->alias]['geoFields']['alias'];
			foreach ($results as $i => $result) {
				$key = null;
				if (!isset($result[0]) && isset($result[$Model->alias])) {
					$key = $Model->alias;
				} elseif (isset($result[0])) {
					$key = 0;
				}
				foreach ($result[$key] as $field => $value) {
					if ($field === $this->distanceField) {
						$results[$i][$geoField]['distance'] = (
							is_array($value) && isset($value[0]) ?
							$value[0]:
							$value
						);
					}
				}
			}
			return $results;
		}
	}

/**
 * parse the $query array for a geo query (sorting by distance)
 *
 * @param Model $Model
 * @param string $query
 * @return void
 * @author David Kullmann
 */
	public function parseGeoQuery(Model $Model, $query = array()) {
		$geo = $this->settings[$Model->alias]['geoFields'];

		$alias = empty($geo['alias']) ? $Model->alias : $geo['alias'];

		$latKey = implode('.', array($alias, $geo['latitude']));
		$lngKey = implode('.', array($alias, $geo['longitude']));

		if (empty($query['conditions'][$latKey]) || empty($query['conditions'][$lngKey])) {
			throw new Exception('Cannot perform a geo search without longitude and latitude');
		}

		$Model->latitude  = $query['latitude']	= $query['conditions'][$latKey];
		$Model->longitude = $query['longitude'] = $query['conditions'][$lngKey];

		unset($query['conditions'][$latKey]);
		unset($query['conditions'][$lngKey]);

		$ds = $Model->getDataSource();
		$useScriptFields = (!empty($ds->config['useScriptFields']));

		$distanceField = sprintf("doc['%s.%s'].distance(%s, %s)",
			$alias,
			$geo['location'],
			$useScriptFields ? 'lat' : $query['latitude'],
			$useScriptFields ? 'lon' : $query['longitude']
		);
		$this->distanceField = $distanceField;

		if (empty($query['fields'])) {
			$query['fields'] = array('_source');
		}

		if ($useScriptFields) {
			$query['script_fields'] = array(
				$this->distanceField => array(
					'lang' => 'groovy',
					'script' => $distanceField,
					'params' => array(
						'lat' => (float) $query['latitude'],
						'lon' => (float) $query['longitude']
					)
				)
			);
		} else {
			$query['fields'] = array_merge($query['fields'], (array)$this->distanceField);
		}

		return $query;
	}

/**
 * Index a document or a list of documents, bypassing Model::save() (much faster)
 *
 * Good for indexing pre-validated data such as data from your DB
 *
 * @param Model $Model
 * @param array $documents
 * @return void
 * @author David Kullmann
 */
	public function index(Model $Model, $documents = array(), $options = array()) {
		$defaults = array('callbacks' => false);
		$options = array_merge($defaults, $options);

		$geoFields = !empty($this->settings[$Model->alias]['geoFields']) ? $this->settings[$Model->alias]['geoFields'] : false;
		if ($geoFields) {
			extract($geoFields);
		}
		$ds = $Model->getDataSource();
		$ds->begin();
		foreach ($documents as $document) {
			if ($geoFields) {
				$document[$alias][$location] = array(
					'lat' => $document[$Model->alias][$latitude],
					'lon' => $document[$Model->alias][$longitude]
				);
			}
			if ($options['callbacks'] === true || $options['callbacks'] === 'before') {
				$Model->set($document);
				$event = new CakeEvent('Model.beforeSave', $Model, array($options));
				list($event->break, $event->breakOn) = array(true, array(false, null));
				$Model->getEventManager()->dispatch($event);
				if (!$event->result) {
					return false;
				}
			}
			$ds->addToDocument($Model, $document);
		}
		$ds->commit();
		return $documents;
	}

/**
 * Find the last model that was synced, override this in your model if you have a different method
 *
 * @param Model $Model
 * @param array $params
 * @return string Date string representing the last model synced to ES
 * @author David Kullmann
 */
	public function lastSync(Model $Model, $params = array()) {
		list($alias, $field) = $this->getModificationField($Model);
		$modificationField = $alias.'.'.$field;
		$fields = array($modificationField);
		$order = array($modificationField => 'DESC');
		$conditions = array('NOT' => array($modificationField => NULL));
		try {
			$result = $Model->find('first', compact('fields', 'order', 'conditions'));
		} catch (Exception $e) {
			$result = null;
		}

		if (empty($result[$alias][$field])) {
			$result = '1970-01-01 00:00:00';
		} else {
			$result = $result[$alias][$field];
		}
		return $result;
	}

/**
 * Build the sync conditions to sync models to ES, override in your Model if necessary
 *
 * @param Model $Model
 * @param string $field
 * @param string $date
 * @param string $params
 * @return void
 * @author David Kullmann
 */
	public function syncConditions(Model $Model, $field, $date, $params = array()) {
		return array($Model->alias.'.'.$field . ' >=' => $date);
	}

/**
 * Get the modification field for this model - override in your Model if necessary
 *
 * @param Model $Model
 * @return void
 * @author David Kullmann
 */
	public function getModificationField(Model $Model) {
		$modificationField = $this->settings[$Model->alias]['modificationField'];
		if (!strpos($modificationField, '.')) {
			$modificationField = $Model->alias.'.'.$modificationField;
		}
		return explode('.', $modificationField);
	}
}
