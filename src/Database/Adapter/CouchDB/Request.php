<?php

namespace Utopia\Database\Adapter\CouchDB;

use Exception;
use Utopia\Database\Adapter\CouchDB\Exception\MethodNotFound;

class Request
{
    /**
     * @var string
     */
    private string $host;

    /**
     * @var int
     */
    private int $port;

    /**
     * @var string
     */
    private string $username;

    /**
     * @var string
     */
    private string $password;

    /**
     * @var bool
     */
    private bool $tls;

    /**
     * @var int
     */
    private int $timeout = 60;

    /**
     * @var array
     */
    private array $methods = ['HEAD', 'GET', 'POST', 'PUT', 'DELETE'];

    public function __construct(string $host, int $port, string $username, string $password, ?int $timeout = null, bool $tls = false)
    {
        $this->host = $host;
        $this->port = $port;
        $this->username = $username;
        $this->password = $password;
        $this->tls = $tls;

        if (!is_null($timeout))
            $this->timeout = $timeout;
    }


    /**
     * @param string $method
     * @param array $data
     * 
     * @return array
     * @throws MethodNotFound
     */
    private function buildContext(string $method, array|null $data): array
    {
        if (!in_array(strtoupper($method), $this->methods, true))
            throw new MethodNotFound('CouchDB does not support '. strtoupper($method). ' method');

        if (!is_null($data)) {
            return array(
                'http' => array(
                    'header' => 'Content-type: application/json',
                    'method' => strtoupper($method),
                    'content' => json_encode($data),
                    'timeout' => $this->timeout,
                    'ignore_errors' => true
                )
            );
        } else {
            return array(
                'http' => array(
                    'header' => 'Content-type: application/json',
                    'method' => strtoupper($method),
                    'timeout' => $this->timeout,
                    'ignore_errors' => true
                )
            );
        }
        
    }

    public function __call($name, $arguments): array
    {
        try {
            $context = stream_context_create($this->buildContext($name, $arguments[0]['data']));
            $url = $this->generateUrl($arguments[0]['uri']);
            $response = file_get_contents($url, false, $context);
            return [
                'code' => $this->parseHeaders($http_response_header)['response_code'],
                'body' => json_decode($response, true)
            ];
        } catch (MethodNotFound $e) {
            throw $e;
        } catch (Exception $e) {
            throw $e;
        }
    }

    /**
     * Generate Url
     * 
     * @param string $uri
     * 
     * @return string
     */
    private function generateUrl(string $uri): string
    {
        $url = '';
        if ($this->tls)
            $url .= 'https://';
        else
            $url .= 'http://';

        $url .= "$this->username:$this->password@$this->host:$this->port";

        return $url . $uri;
    }

    /**
     * Parse response headers
     * 
     * @param array $headers
     * 
     * @return array
     */
    private function parseHeaders(array $headers): array
    {
        $head = array();
        foreach ($headers as $key => $value) {
            $t = explode(':', $value, 2);
            if (isset($t[1]))
                $head[trim($t[0])] = trim($t[1]);
            else {
                $head[] = $value;
                if(preg_match("#HTTP/[0-9\.]+\s+([0-9]+)#", $value, $out))
                    $head['response_code'] = intval($out[1]);
            }
        }
        return $head;
    }
}