<?php

namespace Tests\Unit;

use DateTime;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Index;

final class CountingDatabase extends Database
{
    public function __construct(
        Adapter $adapter,
        Cache $cache,
        private readonly MagicAccessRecorder $recorder,
    ) {
        parent::__construct($adapter, $cache);
    }

    protected ?DateTime $timestamp {
        get {
            $this->recorder->hookRead('Database', 'timestamp');

            return parent::$timestamp::get(); // @phpstan-ignore property.staticAccess, staticMethod.nonObject, return.type
        }
        set {
            parent::$timestamp::set($value); // @phpstan-ignore property.staticAccess, staticMethod.nonObject
        }
    }

    protected bool $filter {
        get {
            $this->recorder->hookRead('Database', 'filter');

            return parent::$filter::get(); // @phpstan-ignore property.staticAccess, staticMethod.nonObject, return.type
        }
        set {
            parent::$filter::set($value); // @phpstan-ignore property.staticAccess, staticMethod.nonObject
        }
    }

    /**
     * @var array<string, bool>|null
     */
    protected ?array $disabledFilters {
        get {
            $this->recorder->hookRead('Database', 'disabledFilters');

            return parent::$disabledFilters::get(); // @phpstan-ignore property.staticAccess, staticMethod.nonObject, return.type
        }
        set {
            parent::$disabledFilters::set($value); // @phpstan-ignore property.staticAccess, staticMethod.nonObject
        }
    }

    protected bool $validate {
        get {
            $this->recorder->hookRead('Database', 'validate');

            return parent::$validate::get(); // @phpstan-ignore property.staticAccess, staticMethod.nonObject, return.type
        }
        set {
            parent::$validate::set($value); // @phpstan-ignore property.staticAccess, staticMethod.nonObject
        }
    }

    protected bool $preserveDates {
        get {
            $this->recorder->hookRead('Database', 'preserveDates');

            return parent::$preserveDates::get(); // @phpstan-ignore property.staticAccess, staticMethod.nonObject, return.type
        }
        set {
            parent::$preserveDates::set($value); // @phpstan-ignore property.staticAccess, staticMethod.nonObject
        }
    }

    protected bool $preserveSequence {
        get {
            $this->recorder->hookRead('Database', 'preserveSequence');

            return parent::$preserveSequence::get(); // @phpstan-ignore property.staticAccess, staticMethod.nonObject, return.type
        }
        set {
            parent::$preserveSequence::set($value); // @phpstan-ignore property.staticAccess, staticMethod.nonObject
        }
    }

    protected bool $skipDuplicates {
        get {
            $this->recorder->hookRead('Database', 'skipDuplicates');

            return parent::$skipDuplicates::get(); // @phpstan-ignore property.staticAccess, staticMethod.nonObject, return.type
        }
        set {
            parent::$skipDuplicates::set($value); // @phpstan-ignore property.staticAccess, staticMethod.nonObject
        }
    }

    #[\Override]
    public function getCollection(string $id): Collection
    {
        $collection = parent::getCollection($id);
        if ($collection->isEmpty()) {
            return $collection;
        }

        $counting = CountingCollection::of($collection, $this->recorder);
        $counting->setAttribute('attributes', \array_map(
            fn (Attribute $attribute): Attribute => CountingAttribute::of($attribute, $this->recorder),
            $collection->getDeclaredAttributes(),
        ));
        $counting->setAttribute('indexes', \array_map(
            fn (Index $index): Index => CountingIndex::of($index, $this->recorder),
            $collection->getIndexes(),
        ));

        return $counting;
    }
}
