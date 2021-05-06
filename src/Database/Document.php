<?php

namespace Utopia\Database;

use ArrayObject;
use Exception;

class Document extends ArrayObject
{
    const SET_TYPE_ASSIGN = 'assign';
    const SET_TYPE_PREPEND = 'prepend';
    const SET_TYPE_APPEND = 'append';

    /**
     * @var bool
     */
    protected $filter = false;

    /**
     * @var bool
     */
    protected $casting = false;

    /**
     * Construct.
     *
     * Construct a new fields object
     *
     * @see ArrayObject::__construct
     *
     * @param array $input
     * @param int    $flags
     * @param string $iterator_class
     */
    public function __construct(array $input = [])
    {
        if(isset($input['$read']) && !is_array($input['$read'])) {
            throw new Exception('$read permission must be of type array');
        }
        
        if(isset($input['$write']) && !is_array($input['$write'])) {
            throw new Exception('$write permission must be of type array');
        }

        foreach ($input as $key => &$value) {
            if (\is_array($value)) {
                if ((isset($value['$id']) || isset($value['$collection']))) {
                    $input[$key] = new self($value);
                } else {
                    foreach ($value as $childKey => $child) {
                        if ((isset($child['$id']) || isset($child['$collection'])) && (!$child instanceof self)) {
                            $value[$childKey] = new self($child);
                        }
                    }
                }
            }
        }

        parent::__construct($input);
    }

    /**
     * @return string
     */
    public function getId(): string
    {
        return $this->getAttribute('$id', '');
    }

    /**
     * @return string
     */
    public function getCollection(): string
    {
        return $this->getAttribute('$collection', '');
    }

    /**
     * @return array
     */
    public function getRead(): array
    {
        return $this->getAttribute('$read', []);
    }

    /**
     * @return array
     */
    public function getWrite(): array
    {
        return $this->getAttribute('$write', []);
    }

    /**
     * Get Document Attributes
     * 
     * @return array
     */
    public function getAttributes(): array
    {
        $attributes = [];

        foreach ($this as $attribute => $value) {
            if(array_key_exists($attribute, ['$id' => true, '$collection' => true, '$read' => true, '$write' => []])) {
                continue;
            }

            $attributes[$attribute] = $value;
        }

        return $attributes;
    }

    /**
     * Get Attribute.
     *
     * Method for getting a specific fields attribute. If $name is not found $default value will be returned.
     *
     * @param string $name
     * @param mixed  $default
     *
     * @return mixed
     */
    public function getAttribute(string $name, $default = null)
    {
        $name = \explode('.', $name);

        $temp = &$this;

        foreach ($name as $key) {
            if (!isset($temp[$key])) {
                return $default;
            }

            $temp = &$temp[$key];
        }

        return $temp;
    }

    /**
     * Set Attribute.
     *
     * Method for setting a specific field attribute
     *
     * @param string $key
     * @param mixed  $value
     * @param string $type
     *
     * @return self
     */
    public function setAttribute(string $key, $value, string $type = self::SET_TYPE_ASSIGN): self
    {
        switch ($type) {
            case self::SET_TYPE_ASSIGN:
                $this[$key] = $value;
                break;
            case self::SET_TYPE_APPEND:
                $this[$key] = (!isset($this[$key]) || !\is_array($this[$key])) ? [] : $this[$key];
                \array_push($this[$key], $value);
                break;
            case self::SET_TYPE_PREPEND:
                $this[$key] = (!isset($this[$key]) || !\is_array($this[$key])) ? [] : $this[$key];
                \array_unshift($this[$key], $value);
                break;
        }

        return $this;
    }

    /**
     * Remove Attribute.
     *
     * Method for removing a specific field attribute
     *
     * @param string $key
     *
     * @return self
     */
    public function removeAttribute(string $key): self
    {
        if (isset($this[$key])) {
            unset($this[$key]);
        }

        return $this;
    }

    /**
     * Search.
     *
     * Get array child by key and value match
     *
     * @param string $key
     * @param mixed $value
     * @param mixed $scope
     *
     * @return mixed
     */
    public function search(string $key, $value, $scope = null)
    {
        $array = (!\is_null($scope)) ? $scope : $this;

        if (\is_array($array)  || $array instanceof self) {
            if (isset($array[$key]) && $array[$key] == $value) {
                return $array;
            }

            foreach ($array as $k => $v) {
                if ((\is_array($v) || $v instanceof self) && (!empty($v))) {
                    $result = $this->search($key, $value, $v);

                    if (!empty($result)) {
                        return $result;
                    }
                } else {
                    if ($k === $key && $v === $value) {
                        return $array;
                    }
                }
            }
        }

        if ($array === $value) {
            return $array;
        }

        return;
    }

    /**
     * Checks if document has data.
     *
     * @return bool
     */
    public function isEmpty(): bool
    {
        return empty($this->getId());
    }

    /**
     * Checks if a document key is set.
     *
     * @param string $key
     *
     * @return bool
     */
    public function isSet($key): bool
    {
        return isset($this[$key]);
    }

    /**
     * Get Document Filter Status
     *
     * @return bool
     */
    public function getFilterStatus(): bool
    {
        return $this->filter;
    }

    /**
     * Set Document Filter Status
     *
     * @param bool $status
     *
     * @return self
     */
    public function setFilterStatus(bool $status): self
    {
        $this->filter = $status;
        return $this;
    }

    /**
     * Get Document Casting Status
     *
     * @return bool
     */
    public function getCastingStatus(): bool
    {
        return $this->casting;
    }

    /**
     * Set Document Casting Status
     *
     * @param bool $status
     *
     * @return self
     */
    public function setCastingStatus(bool $status): self
    {
        $this->casting = $status;
        return $this;
    }

    /**
     * Get Array Copy.
     *
     * Outputs entity as a PHP array
     *
     * @param array $allow
     * @param array $disallow
     *
     * @return array
     */
    public function getArrayCopy(array $allow = [], array $disallow = []): array
    {
        $array = parent::getArrayCopy();

        $output = [];

        foreach ($array as $key => &$value) {
            if (!empty($allow) && !\in_array($key, $allow)) { // Export only allow fields
                continue;
            }

            if (!empty($disallow) && \in_array($key, $disallow)) { // Don't export disallowed fields
                continue;
            }

            if ($value instanceof self) {
                $output[$key] = $value->getArrayCopy($allow, $disallow);
            } elseif (\is_array($value)) {
                foreach ($value as $childKey => &$child) {
                    if ($child instanceof self) {
                        $output[$key][$childKey] = $child->getArrayCopy($allow, $disallow);
                    } else {
                        $output[$key][$childKey] = $child;
                    }
                }

                if (empty($value)) {
                    $output[$key] = $value;
                }
            } else {
                $output[$key] = $value;
            }
        }

        return $output;
    }
}
