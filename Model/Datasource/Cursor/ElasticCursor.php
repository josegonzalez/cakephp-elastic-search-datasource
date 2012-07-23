<?php

/**
 * A generic Cursor/Statement class to wrap options to be passed to an
 * ElasticSource source object to make requests to the server. Results
 * obtained from such request will be returned one by one by this iterator.
 *
 * Useful to wrap searches before actually execute them as the request will only
 * be made when first result is tried to be fetched.
 *
 **/
class ElasticCursor implements Iterator {

/**
 * Internal array iterator that wraps results from Elastic Search
 *
 * @var Iterator
 **/
	protected $_iterator;

/**
 * ElasticSource instance used to make requests to server
 *
 * @var ElasticSource
 **/
	protected $_source;

/**
 * Options to be passed to request for fetching results
 *
 * @var array
 **/
	protected $options = array();

	public function __construct(ElasticSource $source, $options) {
		$this->_source = $source;
		$this->_options = $options;
	}

/**
 * Sends request to ElasticSearch, wraps results in an iterator and returns it
 *
 * @return ArrayIterator
 **/
	public function getIterator() {
		if (empty($this->_iterator)) {
			$method = $this->_options['method'];
			$api = $this->_options['api'];
			$index = $this->_options['type'];
			unset($this->_options['method'], $this->_options['api'], $this->_options['type']);
			$this->_iterator = new ArrayIterator(
				$this->_source->filterResults(
					$this->_source->execute($method, $index, $api, $this->_options)
				)
			);
		}
		return $this->_iterator;
	}

/**
 * Returns current's row value
 *
 * @return mixed
 **/
	public function current() {
		return $this->getIterator()->current();
	}

/**
 * Returns current row key
 *
 * @return string
 **/
	public function key() {
		return $this->getIterator()->key();
	}

/**
 * Forwards internal cursor pointer to next position
 *
 * @return void
 **/
	public function next() {
		return $this->getIterator()->next();
	}

/**
 * Rewinds internal cursor pointer
 *
 * @return void
 **/
	public function rewind() {
		return $this->getIterator()->rewind();
	}

/**
 * Returns whether there are more results to be fetched or not
 *
 * @return boolean
 **/
	public function valid() {
		return $this->getIterator()->valid();
	}
}
