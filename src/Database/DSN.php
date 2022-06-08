<?php

namespace Utopia\Database;


class DSN {
    /**
     * @var string
     */
    protected string $scheme;

    /**
     * @var string
     */
    protected string $user;

    /**
     * @var string
     */
    protected string $password;

    /**
     * @var string
     */
    protected string $host; 

    /**
     * @var string
     */
    protected string $port;

    /**
     * @var string
     */
    protected string $path;

    /**
     * @var string
     */
    protected string $query;

    /**
     * Construct
     *
     * Construct a new DSN object
     * 
     * @param string $dsn
     */
    public function __construct(string $dsn)
    {
        $parts = parse_url($dsn);

        if (!$parts) {
            throw new \InvalidArgumentException("Unable to parse DSN: $dsn");
        }

        $this->scheme = $parts['scheme'] ?? '';
        $this->user = $parts['user'] ?? '';
        $this->password = $parts['pass'] ?? '';
        $this->host = $parts['host'] ?? '';
        $this->port = $parts['port'] ?? '';
        $this->path = $parts['path'] ?? '';
        $this->query = $parts['query'] ?? '';
    }

    /**
     * Return the scheme.
     * 
     * @return string
     */
    public function getScheme(): string 
    {
        return $this->scheme;
    }

    /**
     * Return the user.
     * 
     * @return string
     */
    public function getUser(): string 
    {
        return $this->user;
    }

    /**
     * Return the password.
     * 
     * @return string
     */
    public function getPassword(): string 
    {
        return $this->password;
    }

    /**
     * Return the host
     * 
     * @return string
     */
    public function getHost(): string 
    {
        return $this->host;
    }

    /**
     * Return the port
     * 
     * @return string
     */
    public function getPort(): string
    {
        return $this->port;
    }

    /**
     * Return the path
     * 
     * @return string
     */
    public function getPath(): string 
    {
        return $this->path;
    }

    /**
     * Return the query string
     * 
     * @return string
     */
    public function getQuery(): string 
    {
        return $this->query;
    }
}