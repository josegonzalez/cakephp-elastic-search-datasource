<?php
class ElasticFixture extends CakeTestFixture {
	
/**
 * Require a test datasource
 *
 * @var string
 */
	public $useDbConfig = 'test_index';

/**
 * Override the normal CakeTestFixture::__construct() to get the name
 * from our pattern
 *
 * @author David Kullmann
 */
	public function __construct() {
		$class = get_class($this);
		if ($this->name === null) {
			if (preg_match('/^Elastic(.*)Fixture$/', $class, $matches)) {
				$this->name = $matches[1];
			} else {
				throw new Exception("ElasticFixtures should have the naming pattern Elastic<Model>Fixture ('$class' given)");
			}
		}
		return parent::__construct();
	}
	
	
}
?>