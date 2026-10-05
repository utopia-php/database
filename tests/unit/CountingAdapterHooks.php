<?php

namespace Tests\Unit;

trait CountingAdapterHooks
{
    private ?MagicAccessRecorder $hookRecorder = null;

    public function countHookReads(MagicAccessRecorder $recorder): void
    {
        $this->hookRecorder = $recorder;
    }

    protected int|string|null $tenant {
        get {
            $this->hookRecorder?->hookRead('Adapter', 'tenant');

            return parent::$tenant::get(); // @phpstan-ignore property.staticAccess, staticMethod.nonObject, return.type
        }
        set {
            parent::$tenant::set($value); // @phpstan-ignore property.staticAccess, staticMethod.nonObject
        }
    }

    protected bool $skipDuplicates {
        get {
            $this->hookRecorder?->hookRead('Adapter', 'skipDuplicates');

            return parent::$skipDuplicates::get(); // @phpstan-ignore property.staticAccess, staticMethod.nonObject, return.type
        }
        set {
            parent::$skipDuplicates::set($value); // @phpstan-ignore property.staticAccess, staticMethod.nonObject
        }
    }
}
