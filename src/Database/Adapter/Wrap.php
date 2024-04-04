<?php

namespace Utopia\Database\Adapter;

use Exception;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Database;

class FunctionTimer {
    private $startTimes = [];

    public function startTimer(string $functionName): void {
        $this->startTimes[$functionName] = microtime(true);
    }

    public function endTimer(string $functionName): float {
        $endTime = microtime(true);
        $startTime = $this->startTimes[$functionName] ?? 0;
        unset($this->startTimes[$functionName]);
        return $endTime - $startTime;
    }
}

class Wrap extends Database {
    private $timer;

    public function __construct(Adapter $adapter, Cache $cache, array $filters = []) {
        parent::__construct($adapter, $cache, $filters);
        $this->timer = new FunctionTimer();
    }

    private function startTimer(string $functionName): void {
        $this->timer->startTimer($functionName);
    }

    private function endTimer(string $functionName): float {
        return $this->timer->endTimer($functionName);
    }

    /**
     * @throws Exception
     */
    public function __call($method, $args) {

        var_dump("==== __call ");

        if (method_exists($this, $method)) {
            $this->startTimer($method);
            $result = call_user_func_array([$this, $method], $args);
            $timeTaken = $this->endTimer($method);
            var_dump("$method took $timeTaken seconds");
            return $result;
        } else {
            throw new Exception("Method $method does not exist");
        }
    }
}