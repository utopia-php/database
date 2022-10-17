<?php

namespace Utopia\Database\Adapter;

use Exception;
use Utopia\Database\Adapter;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Duplicate;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;

// Create a Neo4j client using curl and the Neo4j REST API
class Neo4jClient {
    private $host;
    private $port;
    private $username;
    private $password;
    private $db = 'neo4j';

    public function __construct($host, $port, $username, $password) {
        $this->host = $host;
        $this->port = $port;
        $this->username = $username;
        $this->password = $password;
    }

    public function query($query, $params = []) {
        $url = "http://{$this->host}:{$this->port}/db/{$this->db}/tx";
        $headers = [
            'Content-Type: application/json',
            'Accept: application/json; charset=UTF-8',
        ];
        $data = [
            'statement' => $query,
            'parameters' => $params,
        ];
        $ch = curl_init();
        curl_setopt($ch, CURLOPT_URL, $url);
        curl_setopt($ch, CURLOPT_HTTPHEADER, $headers);
        curl_setopt($ch, CURLOPT_USERPWD, "{$this->username}:{$this->password}");
        curl_setopt($ch, CURLOPT_POST, 1);
        curl_setopt($ch, CURLOPT_POSTFIELDS, json_encode($data));
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
        $response = curl_exec($ch);
        $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
        curl_close($ch);

        // Check HTTP code
        if ($httpCode > 299) {
            throw new Exception($response);
        }
        $responseJson = json_decode($response, true);

        // Check if we have any errors in the response
        if (count($responseJson['errors']) > 0) {
            throw new Exception($responseJson['errors'][0]['message']);
        }

        // Transform result data into a more usable format
        $result = $responseJson['results'][0];
        $rows = [];
        foreach ($result['data'] as $row_data) {
            $row = [];
            foreach (array_values($row_data['row']) as $i => $val) {
                $row[$result['columns'][$i]] = $val;
            }
            $rows[] = $row;
        }

        return $rows;
    }
}


class Neo4j extends Adapter
{
    /**
     * @var Neo4jClient
     */
    protected $client;

    /**
     * Constructor.
     *
     * Set connection and settings
     *
     * @param Neo4jClient $client
     */
    public function __construct(Neo4jClient $client)
    {
        $this->client = $client;
    }

    /**
     * Create Database
     *
     * @param string $name
     *
     * @return bool
     */
    public function create(string $name): bool {
        $this->client->query("CREATE DATABASE {$name}");
        return true;
    }

    /**
     * Check if database exists
     * Optionally check if collection exists in database
     *
     * @param string $database database name
     * @param string $collection (optional) collection name
     *
     * @return bool
     */
    public function exists(string $database, ?string $collection): bool 
    {
        $database = $this->filter($database);

        $result = $this->list();
        foreach ($result as $db) {
            if ($db == $database) {
                return true;
            }
        }
        return false;
    }

    /**
     * List Databases
     *
     * @return array
     */
    public function list(): array
    {
        // List all databases using the Neo4jClient
        $result = $this->client->query("SHOW DATABASES");
        $databases = [];
        foreach ($result as $db) {
            $databases[] = $db['name'];
        }
        return $databases;
    }

    /**
     * Delete Database
     *
     * @param string $name
     *
     * @return bool
     */
    public function delete(string $name): bool {
        return true;
    }

}