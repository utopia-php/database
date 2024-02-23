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
     * Execute raw command with no response
     * @param mixed $query
     *
     * @return mixed
     * @throws \Throwable
     */
    public function executeWrite(mixed $query): bool
    {
        $this->query($this->endpoint, $this->secret, $this->database, $query);
        return true;
    }

    /**
     * Execute raw command that returns a response
     * @param mixed $query
     *
     * @return mixed
     * @throws \Throwable
     */
    public function executeRead(mixed $query): mixed
    {
        return $this->query($this->endpoint, $this->secret, $this->database, $query);
    }
}
