# CakePHP Elastic Plugin & ElasticSearch "ElasticSource" Datasource

Conveniently index and access records in ElasticSearch.

## Background

Seamlessly transition from MySQL or another SQL/DBO-backed data source into the amazing ElasticSearch NoSQL indexer. Conforms to CakePHP's ORM but also provides access to facets and queries.

Patches and issues welcome. Please include unit tests.

## Requirements

* PHP >= 5.3
* CakePHP 2.x
* ElasticSearch (http://elasticsearch.org)
* A basic understanding of ElasticSearch.

## Installation

_[Manual]_

* Download this: http://github.com/dkullmann/CakePHP-Elastic-Search-DataSource/zipball/master
* Unzip that download.
* Copy the resulting folder to app/Plugin
* Rename the folder you just copied to Elastic

_[GIT Submodule]_

In your app directory type:

	git submodule add git://github.com/dkullmann/CakePHP-Elastic-Search-DataSource.git Plugin/Elastic
	git submodule update --init

_[GIT Clone]_

In your app directory type

	git clone git://github.com/dkullmann/CakePHP-Elastic-Search-DataSource.git Plugin/Elastic

### Enable plugin

Enable the plugin your `app/Config/bootstrap.php` file:

	CakePlugin::load('Elastic');

If you are already using `CakePlugin::loadAll();`, then this is not necessary.

## Usage

### Setup ElasticSearch

Setup an instance of ElasticSearch to use if you don't have one already.

Open `Config/database.php` and add a datasource called `index`:

	public $index = array(
		'datasource' => 'Elastic.ElasticSource',
		'index' => 'people',
		'port' => 9200
	);

And create your model:

	class Contact extends AppModel {
		
		public $useDbConfig = 'index';
		
	}
	
This will store your `Contact` model in the `people` index as the type `contacts`.
By default the ElasticSearch "type" is the same as the table name you would use for
your model. If you'd like to change the ElasticSearch type then add the variable
`useType` to your model:

	class Contact extends AppModel {
		
		public $useDbConfig = 'index';
		
		public $useType = 'mytype';
		
	}

### Map your model

Elastic Plugin comes with a shell to help you with managing indexes, creating mappings, and indexing records:

	Davids-MacBook-Pro:app dkullmann$ Console/cake Elastic.elastic
	ElasticSearch Plugin Console Commands. Map and index data

	Usage:
	cake elastic.elastic [subcommand] [-h] [-v] [-q]

	Subcommands:

	create_index  Create or alias an index
	mapping       Map a model to ElasticSearch
	index         Index a model into ElasticSearch
	list_sources  Display output from listSources

	To see help on a subcommand use `cake elastic.elastic [subcommand] --help`

	Options:

	--help, -h     Display this help.
	--verbose, -v  Enable verbose output.
	--quiet, -q    Enable quiet output.


To start, create your index

	Console/cake Elastic.elastic create_index test
	
#### Case 1: Your model is already in the 'default' datasource

You can copy the schema with this command:

	Console/cake Elastic.elastic mapping Contact
	
#### Case 2: Your model is in another datasource that responds to `describe`:

	Console/cake Elastic.elastic mapping Contact -d <datasource>
	
#### Case 3: Your model is not yet mapped

You can add a method to your model called elasticMapping to generate the mapping.

Special ElasticSearch types such as geopoint and multi_field are supported.

	class Contact extends AppModel {

		public $useDbConfig = 'index';

		public $_mapping = array(
			'id' => array('type' => 'integer'),
			'name' => array('type' => 'string'),
			'number' => array('type' => 'string'),
			'special_type' => array(
				'type' => 'multi_field',
				'fields' => array(
					'not_analyzed' => array('type' => 'string', 'index' => 'not_analyzed'),
					'analyzed' => array('type' => 'string', 'index' => 'analyzed')
				)
			),
			'created' => array('type' => 'datetime'),
			'modified' => array('type' => 'datetime')
		);
	
		public function elasticMapping() {
			return $this->_mapping;
		}
	}


### Index a few records

If you do not yet have data in MySQL and you are following along with this tutorial you should
create your MySQL tables and data:

	CREATE TABLE `contacts` (
	  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
	  `name` varchar(255) DEFAULT NULL,
	  `number` varchar(255) DEFAULT NULL,
	  `special_type` varchar(255) DEFAULT NULL,
	  `created` datetime NOT NULL,
	  `modified` datetime NOT NULL,
	  PRIMARY KEY (`id`)
	) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8;
	
	INSERT INTO `contacts` (`id`, `name`, `number`, `special_type`, `created`, `modified`)
	VALUES
		(1, 'David', '555-888-1212', 'Multifield', '2012-07-19 20:31:29', '2012-07-19 20:31:29');
	
Otherwise simple start indexing.

To start indexing records you can use this command:

	Console/cake Elastic.elastic index Contact
	
The ElasticShell will add the IndexableBehavior to your model if it's not already added. To
add it permanently add it in your model:

	public $actsAs = array('Elastic.Indexable');
	
By default IndexableBehavior will declare your "modified" field as the field which tracks
when each record was updated or created in order to synchronize it with ElasticSearch.

Test using this command

	curl -XGET 'http://localhost:9200/people/contacts/1
	
The output should include:

	{
	  "_index" : "test",
	  "_type" : "contacts",
	  "_id" : "1",
	  "_version" : 2,
	  "exists" : true, "_source" : {"Contact":{"created":"2012-07-19 20:31:29","id":"1","modified":"2012-07-19 20:31:29","name":"David","number":"555-888-1212","special_type":"Multifield"}}
	}

## CRUD Operations

Because ElasticSource conforms to the CakePHP ORM CRUD operations are easy:

	// Create
	$record = array(
		'id' => 1,
		'name' => 'David',
	);
	
	$this->Model->create($data);
	$this->Model->save();
	
	// Read
	$this->Model->findById(1);
	
	// Update
	$this->Model->save($data);
	
	// Delete
	$this->Model->delete(1);

## Additional

Advanced features available using the IndexableBehavior such as geo location searching and sorting

## Todo

* Have more people use it and tell me what they want / patch it
* Unit tests!
* Re-arrange some logic to conform to the ORM in a more simple / less hacky fashion
* Document IndexableBehavior

## License

Copyright (c) 2012 David Kullmann

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.