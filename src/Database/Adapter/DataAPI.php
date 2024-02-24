<?php

namespace Utopia\Database\Adapter;

use Exception;
use Throwable;
use Utopia\Fetch\Client;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Fetch\FetchException;

trait DataAPI
{
    /**
     * @param array[string]mixed $params
     * 
     * @throws FetchException
     * @throws DatabaseException
     * @throws Exception
     */
    private function query(string $endpoint, string $secret, string $database, string $command, array $params): mixed
    {
        $response = Client::fetch(
            url: $endpoint . '/queries',
            headers: [
                'x-utopia-secret' => $secret,
                'x-utopia-database' => $database,
                'content-type' => 'application/json'
            ],
            method: 'POST',
            body: [
                'command' => $command,
                'parrams' => $params
            ]
        );

        if ($response->getStatusCode() >= 400) {
            if (empty($response->getBody())) {
                throw new Exception('Internal ' . $response->getStatusCode() . ' HTTP error in data api');
            }

            $error = \json_decode($response->getBody(), true);

            try {
                $exception = new $error['type']($error['message'], $error['code'], $error['file'], $error['line']);
                /**
                 * @var DatabaseException $exception
                 */
            } catch(Throwable $err) {
                // Cannot find exception type
                throw new Exception($error['message'], $error['code']);
            }

            throw $exception;
        }

        $body = \json_decode($response->getBody(), false);

        return $body->output ?? '';
    }
}
