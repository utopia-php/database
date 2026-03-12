<?php

namespace Utopia\Database\Adapter\Mongo;

use Utopia\Mongo\Client;

/**
 * Proxy wrapper around the Mongo Client that suppresses transient
 * Swoole recv() warnings to prevent PHPUnit's convertWarningsToExceptions
 * from turning EAGAIN into fatal exceptions.
 *
 * When PHPUnit converts Swoole's recv() EAGAIN warning into an exception,
 * it bypasses the Client's internal retry logic in receive(). This proxy
 * installs a temporary error handler during client calls to convert these
 * specific warnings into silent returns, allowing the Client's own retry
 * mechanism to work properly.
 */
class RetryClient
{
    /**
     * Methods that should be passed through without the error handler
     */
    private const PASSTHROUGH = [
        'isConnected',
        'isReplicaSet',
        'toArray',
        'toObject',
        'createUuid',
    ];

    public function __construct(
        private Client $client,
    ) {
    }

    public function unwrap(): Client
    {
        return $this->client;
    }

    public function __call(string $method, array $arguments): mixed
    {
        if (\in_array($method, self::PASSTHROUGH, true)) {
            return $this->client->$method(...$arguments);
        }

        // Suppress Swoole recv() EAGAIN warnings so the Client's
        // internal receive() retry loop can handle them properly
        \set_error_handler(function (int $errno, string $errstr) {
            if (\str_contains($errstr, 'recv() failed')
                && \str_contains($errstr, 'Resource temporarily unavailable')) {
                return true; // Suppress the warning
            }

            return false; // Let other warnings propagate normally
        });

        try {
            return $this->client->$method(...$arguments);
        } finally {
            \restore_error_handler();
        }
    }

    public function __get(string $name): mixed
    {
        return $this->client->$name;
    }
}
