<?php

namespace Utopia\Database\Loading;

use Utopia\Database\Event;
use Utopia\Database\Hook\Lifecycle;

class NPlusOneDetector implements Lifecycle
{
    /** @var array<string, int> */
    private array $queryCounts = [];

    private int $threshold;

    /** @var callable|null */
    private $onDetected;

    public function __construct(int $threshold = 5, ?callable $onDetected = null)
    {
        $this->threshold = $threshold;
        $this->onDetected = $onDetected;
    }

    public function handle(Event $event, mixed $data): void
    {
        if ($event !== Event::DocumentFind && $event !== Event::DocumentRead) {
            return;
        }

        $collection = '';
        if (\is_string($data)) {
            $collection = $data;
        } elseif ($data instanceof \Utopia\Database\Document) {
            $collection = $data->getCollection();
        }

        if ($collection === '') {
            return;
        }

        $key = "{$event->value}:{$collection}";

        if (! isset($this->queryCounts[$key])) {
            $this->queryCounts[$key] = 0;
        }

        $this->queryCounts[$key]++;

        if ($this->queryCounts[$key] === $this->threshold && $this->onDetected !== null) {
            ($this->onDetected)($collection, $event, $this->queryCounts[$key]);
        }
    }

    /**
     * @return array<string, int>
     */
    public function getQueryCounts(): array
    {
        return $this->queryCounts;
    }

    /**
     * @return array<string, int>
     */
    public function getViolations(): array
    {
        return \array_filter($this->queryCounts, fn (int $count) => $count >= $this->threshold);
    }

    public function reset(): void
    {
        $this->queryCounts = [];
    }
}
