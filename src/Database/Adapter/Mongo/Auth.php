<?php

namespace Utopia\Database\Adapter\Mongo;

use MongoDB\BSON;

class Auth
{
    private string $authcid;
    private string $secret;
    private string $authzid;
    private string $gs2Header;
    private string $cnonce;
    private string $firstMessageBare;
    private string $saltedPassword;
    private string $authMessage;

    public function __construct(array $options)
    {
        $this->authcid = isset($options['authcid']) ? $options['authcid'] : '';
        $this->secret = isset($options['secret']) ? $options['secret'] : '';
        $this->authzid = isset($options['authzid']) ? $options['authzid'] : '';
        $this->service = isset($options['service']) ? $options['service'] : '';

        $this->nonce = base64_encode(random_bytes(32));
    }

    public function start(): array
    {
        $response = $this->createResponse();
        $payload = new BSON\Binary($response, 0);

        return [
            [
                "saslStart" => 1,
                "mechanism" => "SCRAM-SHA-1",
                "payload" => $payload,
                "autoAuthorize" => 1,
                "options" => ["skipEmptyExchange" => true],
            ],
            'admin'
        ];
    }

    /**
     * @param mixed $data
     * @return array
     */
    public function continue($data): array
    {
        $cid = $data->conversationId;
        $token = $data->payload->getData();

        $answer = $this->createResponse($token);
        $payload = new \MongoDB\BSON\Binary($answer, 0);

        return [
            [
                "saslContinue" => 1,
                "conversationId" => $cid,
                "payload" => $payload,
            ],
            'admin'
        ];
    }

    /**
     * @param mixed $challenge
     * @return string
     */
    public function createResponse($challenge = null): string
    {
        $authcid = $this->formatName($this->authcid);
        if (empty($authcid)) {
            return false;
        }

        $authzid = $this->authzid;

        if (!empty($authzid)) {
            $authzid = $this->formatName($authzid);
        }

        if (empty($challenge)) {
            return $this->generateInitialResponse($authcid, $authzid);
        } else {
            return $this->generateResponse($challenge, $this->secret);
        }
    }

    /**
     * Prepare a name for inclusion in a SCRAM response.
     *
     * @param string $username a name to be prepared.
     * @return string the reformated name.
     */
    private function formatName(string $username): string
    {
        return str_replace(['=', ','], ['=3D', '=2C'], $username);
    }

    /**
     * Generate the initial response which can be either sent directly in the first message or as a response to an empty
     * server challenge.
     *
     * @param string $authcid Prepared authentication identity.
     * @param string $authzid Prepared authorization identity.
     * @return string The SCRAM response to send.
     */
    private function generateInitialResponse(string $authcid, string $authzid): string
    {
        $gs2CbindFlag   = 'n,';
        $this->gs2Header = $gs2CbindFlag . (!empty($authzid) ? 'a=' . $authzid : '') . ',';

        // I must generate a client nonce and "save" it for later comparison on second response.
        $this->cnonce = $this->generateCnonce();

        $this->firstMessageBare = 'n=' . $authcid . ',r=' . $this->cnonce;

        return $this->gs2Header . $this->firstMessageBare;
    }

    /**
     * Parses and verifies a non-empty SCRAM challenge.
     *
     * @param  string $challenge    The SCRAM challenge
     * @param  string $password     The password challenge
     * @return string|false The response to send; false in case of wrong challenge or if an initial response has not
     * been generated first.
     */
    private function generateResponse(string $challenge, string $password): string|bool
    {
        $matches = [];

        $serverMessageRegexp = "#^r=([\x21-\x2B\x2D-\x7E/]+)"
            . ",s=((?:[A-Za-z0-9/+]{4})*(?:[A-Za-z0-9/+]{3}=|[A-Za-z0-9/+]{2}==)?)"
            . ",i=([0-9]*)(,[A-Za-z]=[^,])*$#";

        if (!isset($this->cnonce, $this->gs2Header) || !preg_match($serverMessageRegexp, $challenge, $matches)) {
            return false;
        }
        $nonce = $matches[1];
        $salt  = base64_decode($matches[2]);

        if (!$salt) {
            return false;
        }
        $i = intval($matches[3]);

        $cnonce = substr($nonce, 0, strlen($this->cnonce));
        if ($cnonce !== $this->cnonce) {
            return false;
        }

        $channelBinding       = 'c=' . base64_encode($this->gs2Header);
        $finalMessage         = $channelBinding . ',r=' . $nonce;
        $saltedPassword       = $this->hi($password, $salt, $i);
        $this->saltedPassword = $saltedPassword;
        $clientKey            = $this->hmac($saltedPassword, "Client Key", true);
        $storedKey            = $this->hash($clientKey, true);
        $authMessage          = $this->firstMessageBare . ',' . $challenge . ',' . $finalMessage;
        $this->authMessage    = $authMessage;
        $clientSignature      = $this->hmac($storedKey, $authMessage, true);
        $clientProof          = $clientKey ^ $clientSignature;
        $proof                = ',p=' . base64_encode($clientProof);

        return $finalMessage . $proof;
    }

    /**
     * Hi() call, which is essentially PBKDF2 (RFC-2898) with HMAC-H() as the pseudorandom function.
     *
     * @param string $str  The string to hash.
     * @param string $salt The salt value.
     * @param int $i The   iteration count.
     * @return string The hashed string.
     */
    private function hi(string $str, string $salt, int $i): string
    {
        $int1   = "\0\0\0\1";
        $ui     = $this->hmac($str, $salt . $int1, true);
        $result = $ui;
        for ($k = 1; $k < $i; $k++) {
            $ui     = $this->hmac($str, $ui, true);
            $result = $result ^ $ui;
        }
        return $result;
    }

    /**
     * SCRAM has also a server verification step. On a successful outcome, it will send additional data which must
     * absolutely be checked against this function. If this fails, the entity which we are communicating with is probably
     * not the server as it has not access to your ServerKey.
     *
     * @param string $data The additional data sent along a successful outcome.
     * @return bool Whether the server has been authenticated.
     * If false, the client must close the connection and consider to be under a MITM attack.
     */
    public function verify(string $data): bool
    {
        $verifierRegexp = '#^v=((?:[A-Za-z0-9/+]{4})*(?:[A-Za-z0-9/+]{3}=|[A-Za-z0-9/+]{2}==)?)$#';

        $matches = [];
        if (!isset($this->saltedPassword, $this->authMessage) || !preg_match($verifierRegexp, $data, $matches)) {
            // This cannot be an outcome, you never sent the challenge's response.
            return false;
        }

        $verifier                = $matches[1];
        $proposedServerSignature = base64_decode($verifier);
        $serverKey               = $this->hmac($this->saltedPassword, "Server Key", true);
        $serverSignature         = $this->hmac($serverKey, $this->authMessage, true);

        return $proposedServerSignature === $serverSignature;
    }

    /**
     * @return string
     */
    public function getCnonce(): string
    {
        return $this->cnonce;
    }

    /**
     * @return string
     */
    public function getSaltedPassword(): string
    {
        return $this->saltedPassword;
    }

    /**
     * @return string
     */
    public function getAuthMessage(): string
    {
        return $this->authMessage;
    }

    /**
     * @return string
     */
    public function getHashAlgo(): string
    {
        return $this->hashAlgo;
    }

    /**
     * @return string
     */
    private function hash($data): string
    {
        return hash('sha1', $data, true);
    }

    /**
     * @return string
     */
    private function hmac($key, $str, $raw): string
    {
        return hash_hmac('sha1', $str, $key, $raw);
    }

    /**
     * @return string
     */
    private function generateCnonce(): string
    {
        foreach (['/dev/urandom', '/dev/random'] as $file) {
            if (is_readable($file)) {
                return base64_encode(file_get_contents($file, false, null, 0, 32));
            }
        }

        $cnonce = '';

        for ($i = 0; $i < 32; $i++) {
            $cnonce .= chr(mt_rand(0, 255));
        }

        return base64_encode($cnonce);
    }

    /**
     * @return string
     */
    static function encodeCredentials($username, $password): string
    {
        return \md5(\utf8_encode($username . ':mongo:' . $password));
    }
}
