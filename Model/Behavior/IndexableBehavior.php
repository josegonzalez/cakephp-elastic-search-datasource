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
		)
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
			foreach ($results as &$result) {
				foreach ($result[0] as $field => $value) {
					if ($field === $this->distanceField) {
						$result[$this->settings[$Model->alias]['geoFields']['alias']]['distance'] = $value;
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

		$query['latitude']  = $query['conditions'][$latKey];
		$query['longitude'] = $query['conditions'][$lngKey];

		unset($query['conditions'][$latKey]);
		unset($query['conditions'][$lngKey]);

		$this->distanceField = sprintf("doc['%s.%s'].distance(%s, %s)", $alias, $geo['location'], $query['latitude'], $query['longitude']);

		$query['fields'] = array(
			'_source',
			$this->distanceField
		);
		$query['order'] = array($alias.'.'.$geo['location'] => 'ASC');

		return $query;
	}

}
