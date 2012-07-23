<?php

App::uses('ElasticCursor', 'ElasticSearch.Model/Datasource/Cursor');

/**
 * An Iterator to scroll results from Elastic Search using an scroll id provided by
 * search server. Results are returned one at a time transparently while internally,
 * several requests to the server are made to scroll results.
 *
 **/
class ElasticScroll extends AppendIterator {

/**
 * ElasticSource instance
 *
 * @var ElasticSource
 **/
	protected $_source;

/**
 * Total number of rows this iterator contains
 *
 * @var integer
 **/
	protected $_total = 0;

/**
 * Number of results per page. Used for sending scrol requests to the server
 *
 * @var integer
 **/
	protected $_pageSize = 0;

/**
 * Current page this iterator is scrolling
 *
 * @var integer
 **/
	protected $_current = 0;

/**
 * Total number of pages to retrieve given the page size and total results
 *
 * @var integer
 **/
	protected $_totalPages = 1;

/**
 * Scroll id as returned by Elastic Search
 *
 * @var string
 **/
	protected $_scrollId;

/**
 * Options to be passed to subsequent requests to Elastic Search
 *
 * @var array
 **/
	protected $_options;

/**
 * Iterator Constructor
 *
 * @param ElasticSource $source Datasource instance to be used to make subsequent requests to Elastic Search
 * @param array $options should contain 'total', 'limit', 'scrollId' and optionaly all other keys to be passed to
 * subsequent requests to Elastic Search
 * @return void
 **/
	public function __construct(ElasticSource $source, $options = array()) {
		$this->_source = $source;
		$this->_total = $options['total'];
		$this->_pageSize = $options['limit'];
		$this->_scrollId = $options['scrollId'];
		$this->_totalPages = ceil($this->_total / $this->_pageSize);

		unset($options['limit'], $options['scrollId'], $options['total']);
		$this->_options = $options;
		parent::__construct();
	}

/**
 * Overrides valid function from parent iterator class to append new iterators on the fly
 * once the previous one reached its end.
 *
 * @return boolean true if there's still more results to be fetched, flase otherwise
 **/
	public function valid() {
		$valid = parent::valid();
		if (!$valid && $this->_current < $this->_totalPages) {
			$from = $this->_current * $this->_pageSize;
			
			$query = array(
				'scroll_id' => $this->_scrollId,
				'from' => $from,
				'size' => $this->_pageSize
			);

			$this->append(new ElasticCursor($this->_source, compact('query') + $this->_options));
			$this->_current++;
			$valid = true;
		}
		return $valid;
	}

}
