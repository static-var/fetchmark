package federation

import (
	"crypto/ed25519"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

const (
	RequestSignatureDomain  = "fetchmark-federation-request-v1\n"
	ResponseSignatureDomain = "fetchmark-federation-response-v1\n"
)

type requestSignaturePayload struct {
	Version    int    `json:"version"`
	Sender     string `json:"sender"`
	Audience   string `json:"audience"`
	KeyID      string `json:"key_id"`
	Method     string `json:"method"`
	Path       string `json:"path"`
	Timestamp  string `json:"timestamp"`
	Nonce      string `json:"nonce"`
	BodySHA256 string `json:"body_sha256"`
}

type responseSignaturePayload struct {
	Version       int    `json:"version"`
	Sender        string `json:"sender"`
	Audience      string `json:"audience"`
	KeyID         string `json:"key_id"`
	Method        string `json:"method"`
	Path          string `json:"path"`
	Timestamp     string `json:"timestamp"`
	Nonce         string `json:"nonce"`
	Status        int    `json:"status"`
	RequestNonce  string `json:"request_nonce"`
	RequestSHA256 string `json:"request_sha256"`
	BodySHA256    string `json:"body_sha256"`
}

func KeyID(publicKey ed25519.PublicKey) string {
	sum := sha256.Sum256(publicKey)
	return hex.EncodeToString(sum[:])
}

// SignRequest returns a deterministic canonical envelope for the supplied
// nonce and timestamp. Callers must generate a fresh cryptographically random
// NonceBytes-byte nonce for every request.
func SignRequest(request SearchRequest, sender, audience string, timestamp time.Time, nonce []byte, privateKey ed25519.PrivateKey) ([]byte, error) {
	body, err := EncodeSearchRequest(request)
	if err != nil {
		return nil, err
	}
	if err := validateID("sender", sender); err != nil {
		return nil, invalidRequest(err)
	}
	if err := validateID("audience", audience); err != nil {
		return nil, invalidRequest(err)
	}
	formattedTime, err := canonicalTimestamp(timestamp)
	if err != nil {
		return nil, invalidRequest(err)
	}
	encodedNonce, err := encodeNonce(nonce)
	if err != nil {
		return nil, invalidRequest(err)
	}
	publicKey, err := publicFromPrivate(privateKey)
	if err != nil {
		return nil, invalidSignature(err)
	}

	envelope := SignedRequest{
		Version: ProtocolVersion, Sender: sender, Audience: audience, KeyID: KeyID(publicKey),
		Method: SearchMethod, Path: SearchPath, Timestamp: formattedTime, Nonce: encodedNonce,
		BodySHA256: digest(body), Body: cloneBytes(body),
	}
	signature, err := signPayload(RequestSignatureDomain, requestPayload(envelope), privateKey)
	if err != nil {
		return nil, err
	}
	envelope.Signature = signature
	return json.Marshal(envelope)
}

// DecodeRequest strictly validates and size-bounds a request without applying
// trust or signature verification. Use VerifyRequest at a trust boundary.
func DecodeRequest(raw []byte) (SignedRequest, SearchRequest, error) {
	return decodeRequest(raw)
}

func VerifyRequest(raw []byte, publicKey ed25519.PublicKey, expected Verification) (VerifiedRequest, error) {
	envelope, request, err := decodeRequest(raw)
	if err != nil {
		return VerifiedRequest{}, err
	}
	if err := validateVerification(expected, envelope.Timestamp); err != nil {
		return VerifiedRequest{}, invalidRequest(err)
	}
	if subtle.ConstantTimeCompare([]byte(envelope.Sender), []byte(expected.Sender)) != 1 ||
		subtle.ConstantTimeCompare([]byte(envelope.Audience), []byte(expected.Audience)) != 1 {
		return VerifiedRequest{}, invalidSignature(errors.New("sender or audience does not match expected context"))
	}
	if err := verifyKeyAndSignature(publicKey, envelope.KeyID, envelope.Signature, RequestSignatureDomain, requestPayload(envelope)); err != nil {
		return VerifiedRequest{}, err
	}
	return VerifiedRequest{Envelope: cloneRequestEnvelope(envelope), Search: request, Digest: digest(raw), nonce: mustDecodeNonce(envelope.Nonce)}, nil
}

// SignResponse binds a success response to the exact request bytes. The
// response sender/audience are derived by reversing the request identities.
func SignResponse(response SearchResponse, requestRaw []byte, timestamp time.Time, nonce []byte, privateKey ed25519.PrivateKey) ([]byte, error) {
	requestEnvelope, _, err := decodeRequest(requestRaw)
	if err != nil {
		return nil, err
	}
	body, err := EncodeSearchResponse(response)
	if err != nil {
		return nil, err
	}
	formattedTime, err := canonicalTimestamp(timestamp)
	if err != nil {
		return nil, invalidResponse(err)
	}
	encodedNonce, err := encodeNonce(nonce)
	if err != nil {
		return nil, invalidResponse(err)
	}
	publicKey, err := publicFromPrivate(privateKey)
	if err != nil {
		return nil, invalidSignature(err)
	}

	envelope := SignedResponse{
		Version: ProtocolVersion, Sender: requestEnvelope.Audience, Audience: requestEnvelope.Sender,
		KeyID: KeyID(publicKey), Method: requestEnvelope.Method, Path: requestEnvelope.Path,
		Timestamp: formattedTime, Nonce: encodedNonce, Status: 200,
		RequestNonce: requestEnvelope.Nonce, RequestSHA256: digest(requestRaw),
		BodySHA256: digest(body), Body: cloneBytes(body),
	}
	signature, err := signPayload(ResponseSignatureDomain, responsePayload(envelope), privateKey)
	if err != nil {
		return nil, err
	}
	envelope.Signature = signature
	return json.Marshal(envelope)
}

// DecodeResponse strictly validates and size-bounds a response without
// applying trust, signature, or request-binding verification.
func DecodeResponse(raw []byte) (SignedResponse, SearchResponse, error) {
	return decodeResponse(raw)
}

func VerifyResponse(raw, exactRequestRaw []byte, publicKey ed25519.PublicKey, expected Verification) (VerifiedResponse, error) {
	requestEnvelope, _, err := decodeRequest(exactRequestRaw)
	if err != nil {
		return VerifiedResponse{}, err
	}
	envelope, response, err := decodeResponse(raw)
	if err != nil {
		return VerifiedResponse{}, err
	}
	if err := validateVerification(expected, envelope.Timestamp); err != nil {
		return VerifiedResponse{}, invalidResponse(err)
	}
	if subtle.ConstantTimeCompare([]byte(envelope.Sender), []byte(expected.Sender)) != 1 ||
		subtle.ConstantTimeCompare([]byte(envelope.Audience), []byte(expected.Audience)) != 1 {
		return VerifiedResponse{}, invalidSignature(errors.New("sender or audience does not match expected context"))
	}
	if envelope.Method != requestEnvelope.Method || envelope.Path != requestEnvelope.Path ||
		subtle.ConstantTimeCompare([]byte(envelope.Sender), []byte(requestEnvelope.Audience)) != 1 ||
		subtle.ConstantTimeCompare([]byte(envelope.Audience), []byte(requestEnvelope.Sender)) != 1 ||
		subtle.ConstantTimeCompare([]byte(envelope.RequestNonce), []byte(requestEnvelope.Nonce)) != 1 ||
		subtle.ConstantTimeCompare([]byte(envelope.RequestSHA256), []byte(digest(exactRequestRaw))) != 1 {
		return VerifiedResponse{}, invalidSignature(errors.New("response is not bound to the exact request"))
	}
	if err := verifyKeyAndSignature(publicKey, envelope.KeyID, envelope.Signature, ResponseSignatureDomain, responsePayload(envelope)); err != nil {
		return VerifiedResponse{}, err
	}
	return VerifiedResponse{Envelope: cloneResponseEnvelope(envelope), Search: response, Digest: digest(raw), nonce: mustDecodeNonce(envelope.Nonce)}, nil
}

func decodeRequest(raw []byte) (SignedRequest, SearchRequest, error) {
	if len(raw) == 0 || len(raw) > MaxRequestBytes {
		return SignedRequest{}, SearchRequest{}, invalidRequest(fmt.Errorf("encoded size must be 1..%d bytes", MaxRequestBytes))
	}
	var envelope SignedRequest
	if err := decodeStrictJSON(raw, &envelope); err != nil {
		return SignedRequest{}, SearchRequest{}, invalidRequest(err)
	}
	if err := validateRequestEnvelope(envelope); err != nil {
		return SignedRequest{}, SearchRequest{}, invalidRequest(err)
	}
	request, err := DecodeSearchRequest(envelope.Body)
	if err != nil {
		return SignedRequest{}, SearchRequest{}, err
	}
	if subtle.ConstantTimeCompare([]byte(envelope.BodySHA256), []byte(digest(envelope.Body))) != 1 {
		return SignedRequest{}, SearchRequest{}, invalidRequest(errors.New("body_sha256 does not match exact body bytes"))
	}
	return cloneRequestEnvelope(envelope), request, nil
}

func decodeResponse(raw []byte) (SignedResponse, SearchResponse, error) {
	if len(raw) == 0 || len(raw) > MaxResponseBytes {
		return SignedResponse{}, SearchResponse{}, invalidResponse(fmt.Errorf("encoded size must be 1..%d bytes", MaxResponseBytes))
	}
	var envelope SignedResponse
	if err := decodeStrictJSON(raw, &envelope); err != nil {
		return SignedResponse{}, SearchResponse{}, invalidResponse(err)
	}
	if err := validateResponseEnvelope(envelope); err != nil {
		return SignedResponse{}, SearchResponse{}, invalidResponse(err)
	}
	response, err := DecodeSearchResponse(envelope.Body)
	if err != nil {
		return SignedResponse{}, SearchResponse{}, err
	}
	if subtle.ConstantTimeCompare([]byte(envelope.BodySHA256), []byte(digest(envelope.Body))) != 1 {
		return SignedResponse{}, SearchResponse{}, invalidResponse(errors.New("body_sha256 does not match exact body bytes"))
	}
	return cloneResponseEnvelope(envelope), response, nil
}

func validateRequestEnvelope(envelope SignedRequest) error {
	if envelope.Version != ProtocolVersion {
		return fmt.Errorf("version must be %d", ProtocolVersion)
	}
	if err := validateID("sender", envelope.Sender); err != nil {
		return err
	}
	if err := validateID("audience", envelope.Audience); err != nil {
		return err
	}
	if envelope.Sender == envelope.Audience {
		return errors.New("sender and audience must differ")
	}
	if err := validateDigest("key_id", envelope.KeyID); err != nil {
		return err
	}
	if envelope.Method != SearchMethod || envelope.Path != SearchPath {
		return errors.New("method and path must identify the exact v1 search route")
	}
	if _, err := parseCanonicalTimestamp(envelope.Timestamp); err != nil {
		return fmt.Errorf("timestamp: %w", err)
	}
	if _, err := decodeNonce(envelope.Nonce); err != nil {
		return err
	}
	if err := validateDigest("body_sha256", envelope.BodySHA256); err != nil {
		return err
	}
	if len(envelope.Body) == 0 {
		return errors.New("body is required")
	}
	if _, err := decodeSignature(envelope.Signature); err != nil {
		return err
	}
	return nil
}

func validateResponseEnvelope(envelope SignedResponse) error {
	if envelope.Version != ProtocolVersion {
		return fmt.Errorf("version must be %d", ProtocolVersion)
	}
	if err := validateID("sender", envelope.Sender); err != nil {
		return err
	}
	if err := validateID("audience", envelope.Audience); err != nil {
		return err
	}
	if envelope.Sender == envelope.Audience {
		return errors.New("sender and audience must differ")
	}
	if err := validateDigest("key_id", envelope.KeyID); err != nil {
		return err
	}
	if envelope.Method != SearchMethod || envelope.Path != SearchPath {
		return errors.New("method and path must identify the exact v1 search route")
	}
	if _, err := parseCanonicalTimestamp(envelope.Timestamp); err != nil {
		return fmt.Errorf("timestamp: %w", err)
	}
	if _, err := decodeNonce(envelope.Nonce); err != nil {
		return err
	}
	if envelope.Status != 200 {
		return errors.New("status must be 200 for the v1 search response")
	}
	if _, err := decodeNonce(envelope.RequestNonce); err != nil {
		return fmt.Errorf("request_nonce: %w", err)
	}
	if err := validateDigest("request_sha256", envelope.RequestSHA256); err != nil {
		return err
	}
	if err := validateDigest("body_sha256", envelope.BodySHA256); err != nil {
		return err
	}
	if len(envelope.Body) == 0 {
		return errors.New("body is required")
	}
	if _, err := decodeSignature(envelope.Signature); err != nil {
		return err
	}
	return nil
}

func requestPayload(envelope SignedRequest) requestSignaturePayload {
	return requestSignaturePayload{envelope.Version, envelope.Sender, envelope.Audience, envelope.KeyID, envelope.Method, envelope.Path, envelope.Timestamp, envelope.Nonce, envelope.BodySHA256}
}

func responsePayload(envelope SignedResponse) responseSignaturePayload {
	return responseSignaturePayload{envelope.Version, envelope.Sender, envelope.Audience, envelope.KeyID, envelope.Method, envelope.Path, envelope.Timestamp, envelope.Nonce, envelope.Status, envelope.RequestNonce, envelope.RequestSHA256, envelope.BodySHA256}
}

func signPayload(domain string, payload any, privateKey ed25519.PrivateKey) (string, error) {
	canonical, err := json.Marshal(payload)
	if err != nil {
		return "", invalidSignature(err)
	}
	message := append([]byte(domain), canonical...)
	return base64.StdEncoding.EncodeToString(ed25519.Sign(privateKey, message)), nil
}

func verifyKeyAndSignature(publicKey ed25519.PublicKey, keyID, encodedSignature, domain string, payload any) error {
	if len(publicKey) != ed25519.PublicKeySize {
		return invalidSignature(errors.New("public key has invalid length"))
	}
	if subtle.ConstantTimeCompare([]byte(KeyID(publicKey)), []byte(keyID)) != 1 {
		return invalidSignature(errors.New("key_id does not match public key"))
	}
	signature, err := decodeSignature(encodedSignature)
	if err != nil {
		return invalidSignature(err)
	}
	canonical, _ := json.Marshal(payload)
	message := append([]byte(domain), canonical...)
	if !ed25519.Verify(publicKey, message, signature) {
		return invalidSignature(errors.New("signature verification failed"))
	}
	return nil
}

func publicFromPrivate(privateKey ed25519.PrivateKey) (ed25519.PublicKey, error) {
	if len(privateKey) != ed25519.PrivateKeySize {
		return nil, errors.New("private key has invalid length")
	}
	canonical := ed25519.NewKeyFromSeed(privateKey.Seed())
	if subtle.ConstantTimeCompare(canonical, privateKey) != 1 {
		return nil, errors.New("private key is not canonical")
	}
	publicKey, ok := privateKey.Public().(ed25519.PublicKey)
	if !ok || len(publicKey) != ed25519.PublicKeySize {
		return nil, errors.New("private key has invalid public key")
	}
	return publicKey, nil
}

func encodeNonce(nonce []byte) (string, error) {
	if len(nonce) != NonceBytes {
		return "", fmt.Errorf("nonce must contain exactly %d bytes", NonceBytes)
	}
	return base64.RawURLEncoding.EncodeToString(nonce), nil
}

func decodeNonce(encoded string) ([]byte, error) {
	decoded, err := base64.RawURLEncoding.Strict().DecodeString(encoded)
	if err != nil || len(decoded) != NonceBytes || base64.RawURLEncoding.EncodeToString(decoded) != encoded {
		return nil, fmt.Errorf("nonce must be canonical unpadded base64url for %d bytes", NonceBytes)
	}
	return decoded, nil
}

func mustDecodeNonce(encoded string) []byte { decoded, _ := decodeNonce(encoded); return decoded }

func decodeSignature(encoded string) ([]byte, error) {
	decoded, err := base64.StdEncoding.Strict().DecodeString(encoded)
	if err != nil || len(decoded) != ed25519.SignatureSize || base64.StdEncoding.EncodeToString(decoded) != encoded {
		return nil, errors.New("signature must be canonical padded base64 for 64 bytes")
	}
	return decoded, nil
}

func cloneRequestEnvelope(value SignedRequest) SignedRequest {
	value.Body = cloneBytes(value.Body)
	return value
}
func cloneResponseEnvelope(value SignedResponse) SignedResponse {
	value.Body = cloneBytes(value.Body)
	return value
}
