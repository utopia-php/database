<?php

namespace Utopia\Database\Adapter;

class DataAPIMariaDB extends MariaDB
{
    use DataAPI;

    protected string $endpoint;
    protected string $secret;
    protected string $database;

    /**
     * Constructor.
     *
     * Set connection and settings
     *
     * @param string $endpoint
     * @param string $secret
     */
    public function __construct(string $endpoint, string $secret, string $database)
    {
        $this->endpoint = $endpoint;
        $this->secret = $secret;
        $this->database = $database;
    }

    /**
     * Execute raw command
     * @param mixed $query
     *
     * @return mixed
     * @throws \Throwable
     */
    public function execute(mixed $query): mixed
    {
        return $this->query($this->endpoint, $this->secret, $this->database, $query);
    }
}
