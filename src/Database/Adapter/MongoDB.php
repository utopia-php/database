<?php

namespace Utopia\Database\Adapter;

use Exception;
use Utopia\Database\Adapter;
use MongoDB\Client;
use MongoDB\Database;

class MongoDB extends Adapter
{
    /**
     * @var Client
     */
    protected $client;

    /**
     * @var Database
     */
    protected $database;

    /**
     * Constructor.
     *
     * Set connection and settings
     *
     * @param Client $client
     */
    public function __construct(Client $client)
    {
        $this->client = $client;
    }

    /**
     * Create Database
     * 
     * @param string $name
     * @return bool
     */
    public function create(string $name): bool
    {
        return (!!$this->getDatabase()->createCollection($name));
    }

    /**
     * Delete Database
     * 
     * @param string $name
     * @return bool
     */
    public function delete(string $name): bool
    {
        return (!!$this->getDatabase()->dropCollection($name));
    }

    /**
     * @return Database
     *
     * @throws Exception
     */
    protected function getDatabase()
    {
        if($this->database) {
            return $this->database;
        }

        $namespace = $this->getNamespace();
        
        return $this->client->$namespace;
    }

    /**
     * @return Client
     *
     * @throws Exception
     */
    protected function getClient()
    {
        return $this->client;
    }
}