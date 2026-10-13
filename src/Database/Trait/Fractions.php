<?php

namespace Utopia\Database\Trait;

use Utopia\Database\Arithmetic\Fractional;
use Utopia\Database\Change;
use Utopia\Database\Collection;
use Utopia\Database\Document;
use Utopia\Database\Exception\Limit as LimitException;
use Utopia\Database\Exception\Structure as StructureException;
use Utopia\Database\Exception\Type as TypeException;
use Utopia\Database\Operator;
use Utopia\Database\Validator\BigInt;
use Utopia\Database\Validator\PartialStructure;

/**
 * Fractional amounts and limits on integer attributes while fractions are rounded, each answered from the stored
 * value as the adapter's engine answered it on database 7.4.1.
 */
trait Fractions
{
    /**
     * Whether a counter change on an integer attribute is answered from the stored value, and the bound to hold it
     * to.
     *
     * @return array{bool, int|float|string|null}
     *
     * @throws TypeException
     */
    private function prepareIntegerChange(int|float|string $value, int|float|string|null $bound, bool $increase): array
    {
        if ($this->isExactChange($value, $bound)) {
            if ($bound !== null) {
                $this->assertNumericBound($bound, $increase ? 'Max' : 'Min');
            }

            return [true, $bound];
        }

        $this->assertIntegerChange($value);

        return [false, $bound === null ? null : $this->integerLimit($bound, $increase)];
    }

    /**
     * A whole change whose bound is not whole is answered from the stored value only where the engine refused a
     * fractional counter, which refused that bound after the limit check.
     */
    private function isExactChange(int|float|string $value, int|float|string|null $bound): bool
    {
        if (Fractional::wholeValue($value) === null && ! Fractional::isFractional($value)) {
            return false;
        }

        if (! BigInt::isIntegerValue($value)) {
            return true;
        }

        return $bound !== null
            && \is_numeric($bound)
            && Fractional::wholeValue($bound) === null
            && $this->adapter->arithmetic()->refusesCounterFractions;
    }

    /**
     * @throws TypeException
     */
    private function integerLimit(int|float|string $bound, bool $maximum): int|string
    {
        $whole = ($maximum ? Fractional::maximum($bound) : Fractional::minimum($bound)) ?? $bound;

        return $this->integerBound($whole, $maximum ? 'Max' : 'Min');
    }

    /**
     * @throws TypeException
     */
    private function assertNumericBound(int|float|string $bound, string $name): void
    {
        if (! \is_numeric($bound) || (\is_float($bound) && \is_nan($bound))) {
            throw new TypeException($name.' must be an integer.');
        }
    }

    /**
     * The whole-number change to apply and the exact result to return. The bound is checked before the engine's
     * own refusals, in the order database 7.4.1 checked them.
     *
     * @return array{int|string, float}
     *
     * @throws LimitException
     * @throws TypeException
     */
    private function exactChange(mixed $stored, int|float|string $value, int|float|string|null $bound, bool $increase): array
    {
        $stored ??= 0;
        if (! BigInt::isIntegerValue($stored)) {
            throw new TypeException('Attribute value must be an integer.');
        }
        $stored = BigInt::toNative($stored);

        if ($bound !== null && Fractional::exceeds($stored, $value, $bound, $increase)) {
            throw new LimitException('Attribute value exceeds '.($increase ? 'maximum' : 'minimum').' limit: '.$bound);
        }

        $arithmetic = $this->adapter->arithmetic();
        $change = (new Fractional($arithmetic))->counter($stored, $value, $increase);

        if ($bound !== null && $arithmetic->refusesCounterFractions && Fractional::wholeValue($bound) === null) {
            throw new TypeException(($increase ? 'Max' : 'Min').' must be an integer.');
        }

        return [$change, $increase ? (float) $stored + (float) $value : (float) $stored - (float) $value];
    }

    /**
     * Cuts the fractional items a document adds to an integer array toward zero, as every engine read them back,
     * and lists the keys of its operators on single integers that need the stored value.
     *
     * @return list<string>
     */
    private function prepareFractions(Collection $collection, Document $document): array
    {
        $keys = [];
        foreach ($collection->attributes() as $attribute) {
            $value = $document->getAttribute($attribute->key);
            if (! $value instanceof Operator || ! $attribute->isInteger()) {
                continue;
            }

            if ($attribute->array) {
                $document->setAttribute($attribute->key, Fractional::whole($value));
            } elseif (Fractional::needsStored($value)) {
                $keys[] = $attribute->key;
            }
        }

        return $keys;
    }

    /**
     * Answers a document's fractional operators from the stored document it changes; strict validation refuses
     * any the engine refused.
     */
    private function resolveFractions(Collection $collection, Document $document, Document $stored): void
    {
        $keys = $this->prepareFractions($collection, $document);
        if ($keys !== []) {
            $document->setAttributes(self::answerFractions($document, $keys, $stored, new Fractional($this->adapter->arithmetic())));
        }
    }

    /**
     * The operators of $changes under $keys answered from the document $target names, read locked for the write;
     * a document not stored yet is answered from zero. Each document is its own locked read: no adapter reads
     * several documents locked.
     *
     * @param  list<string>  $keys
     * @return array<string, Operator>
     *
     * @throws StructureException when the engine refused an operator
     */
    private function lockedFractions(Collection $collection, Document $changes, array $keys, Document $target, Fractional $fractional): array
    {
        $stored = $this->readLocked($collection, $target);
        $resolved = self::answerFractions($changes, $keys, $stored, $fractional);
        $this->assertResolvedFractions($collection, $resolved, $stored);

        return $resolved;
    }

    /**
     * @param  array<Change>  $chunk
     * @param  array<string, list<string>>  $keys  The fractional keys of each upserted document, by its identity
     *
     * @throws StructureException when the engine refused an operator
     */
    private function resolveUpsertFractions(Collection $collection, array $chunk, array $keys): void
    {
        $fractional = new Fractional($this->adapter->arithmetic());
        foreach ($chunk as $change) {
            $documentKeys = $keys[$this->getDocumentIdentity($change->new)] ?? [];
            if ($documentKeys === []) {
                continue;
            }

            $resolved = $this->lockedFractions($collection, $change->new, $documentKeys, $change->new, $fractional);
            $change->new->setAttributes($this->castBefore($collection, new Document($resolved))->getArrayCopy());
        }
    }

    /**
     * @param  list<string>  $keys
     * @return array<string, Operator>
     */
    private static function answerFractions(Document $changes, array $keys, Document $stored, Fractional $fractional): array
    {
        $resolved = [];
        foreach ($keys as $key) {
            $operator = $changes->getAttribute($key);
            $current = $stored->getAttribute($key);
            if ($operator instanceof Operator) {
                $answerable = $current === null || \is_int($current) || \is_float($current) || \is_string($current);
                $resolved[$key] = ($answerable ? $fractional->operator($operator, $current) : $operator)->setAttribute($key);
            }
        }

        return $resolved;
    }

    /**
     * @param  array<string, Operator>  $operators
     *
     * @throws StructureException
     */
    private function assertResolvedFractions(Collection $collection, array $operators, Document $stored): void
    {
        if (! $this->validation()->get()) {
            return;
        }

        $validator = new PartialStructure(
            collection: $collection,
            profile: $this->profile(),
            currentDocument: $stored->isEmpty() ? null : $stored,
        );
        if (! $validator->isValid(new Document($operators))) {
            throw new StructureException($validator->getDescription());
        }
    }

    /**
     * Read under the document's own tenant and whatever the caller may read.
     */
    private function readLocked(Collection $collection, Document $document): Document
    {
        $read = fn (): Document => $this->authorization->skip(fn () => $this->silent(
            fn () => $this->readDocument($collection, $document->getId(), forUpdate: true)
        ));
        $tenant = $document->getTenant();

        return $this->hasSharedTables() && $this->isTenantPerDocument() && $tenant !== null
            ? $this->withTenant($tenant, $read)
            : $read();
    }
}
