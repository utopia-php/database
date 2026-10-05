<?php

namespace Tests\Unit;

final class MagicAccessRecorder
{
    /**
     * @var list<string>
     */
    public private(set) array $reads = [];

    /**
     * @var list<string>
     */
    public private(set) array $writes = [];

    /**
     * @var list<string>
     */
    public private(set) array $hookReads = [];

    private bool $recording = false;

    public function start(): void
    {
        $this->reads = [];
        $this->writes = [];
        $this->hookReads = [];
        $this->recording = true;
    }

    public function stop(): void
    {
        $this->recording = false;
    }

    public function read(string $owner, string $property): void
    {
        if ($this->recording) {
            $this->reads[] = $owner.'->'.$property;
        }
    }

    public function write(string $owner, string $property): void
    {
        if ($this->recording) {
            $this->writes[] = $owner.'->'.$property;
        }
    }

    public function hookRead(string $owner, string $property): void
    {
        if ($this->recording) {
            $this->hookReads[] = $owner.'::$'.$property;
        }
    }
}
