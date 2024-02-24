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
     * @param array[string]mixed $params
     *
     * @return bool
     * @throws \Throwable
     */
    public function executeWrite(mixed $query, array $params): bool
    {
        $this->query($this->endpoint, $this->secret, $this->database, $query, $params);
        return true;
    }

    /**
     * Execute raw command and get amount of affected documents
     * @param mixed $query
     * @param array[string]mixed $params
     *
     * @return int
     * @throws \Throwable
     */
    public function executeWriteWithCount(mixed $query, array $params): int
    {
        $this->query($this->endpoint, $this->secret, $this->database, $query, $params);
        return 1;
    }

    /**
     * Execute raw command that returns a response
     * @param mixed $query
     * @param array[string]mixed $params
     *
     * @return mixed
     * @throws \Throwable
     */
    public function executeRead(mixed $query, array $params): mixed
    {
        return $this->query($this->endpoint, $this->secret, $this->database, $query, $params);
    }
}
