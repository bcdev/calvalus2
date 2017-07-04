#include <openssl/rsa.h>
#include <openssl/bio.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/ossl_typ.h>
#include <openssl/x509.h>
#include <string.h>
#include <apr-1.0/apr_base64.h>

void printhex(unsigned char* buffer, unsigned int length) {
  char hexlhash[length*2+1];
  int i;
  for (i=0; i<length; ++i) {
    sprintf(hexlhash+2*i, "%02x", buffer[i]);
  }
  printf("%s\n", hexlhash);
}

int do_hash(char* str, unsigned char* hash) {
  
  OpenSSL_add_all_digests();
  const EVP_MD* digest = EVP_get_digestbyname("sha256");
  unsigned int hashlen;
  EVP_MD_CTX* ctx = EVP_MD_CTX_create();
  EVP_DigestInit_ex(ctx, digest, NULL);
  EVP_DigestUpdate(ctx, str, strlen(str));
  EVP_DigestFinal_ex(ctx, hash, &hashlen);
  EVP_MD_CTX_destroy(ctx);
  return hashlen;
}

RSA* read_pub(const char* filename) {
  BIO* bio = BIO_new_file(filename, "rb");
  EVP_PKEY* pkey = d2i_PUBKEY_bio(bio, NULL);
  RSA* rsa = EVP_PKEY_get1_RSA(pkey);
  if (rsa == NULL) {
    int e = ERR_get_error();
    printf("%s\n", ERR_error_string(e, NULL));
  }
  OPENSSL_free(pkey);
  BIO_free(bio);
  return rsa;
}

RSA* read_priv(const char* filename) {
  BIO* bio = BIO_new_file(filename, "rb");
  EVP_PKEY* pkey = d2i_PrivateKey_bio(bio, NULL);
  RSA* rsa = EVP_PKEY_get1_RSA(pkey);
  if (rsa == NULL) {
    int e = ERR_get_error();
    printf("%s\n", ERR_error_string(e, NULL));
  }
  OPENSSL_free(pkey);
  BIO_free(bio);
  return rsa;
}

X509* read_cert(const char* filename) {
  BIO* in=BIO_new(BIO_s_file_internal());
  BIO_read_filename(in, filename);
  X509* cer=NULL;
  PEM_read_bio_X509(in, &cer, 0, NULL);
  BIO_free(in);
  //EVP_PKEY* key = X509_get_pubkey(cer);
  //XSECCryptoKey* ret=new OpenSSLCryptoKeyRSA(key);
  return cer;
}

EVP_PKEY* read_cert_pkey(const char* filename) {
  BIO* in=BIO_new(BIO_s_file_internal());
  BIO_read_filename(in, filename);
  X509* cer=NULL;
  PEM_read_bio_X509(in, &cer, 0, NULL);
  BIO_free(in);
  EVP_PKEY* result = X509_get_pubkey(cer);
  // TBD free cer?
  return result;
}

char* testDecrypt() {
  ERR_load_crypto_strings();

  /* read keys from files */

  RSA* calvalus_pub_rsa = read_pub("calvalus_pub.der");
  RSA* calvalus_priv_rsa = read_priv("calvalus_priv.der");
  RSA* cas_pub_rsa = read_pub("cas_pub.der");
  RSA* cas_priv_rsa = read_priv("cas_priv.der");

  // read calvalus token made of base64-encoded aeskey and hash-and-saml

  FILE* f = fopen("calvalus_token.dat", "rb");
  char calvalusToken[8192];
  int i = 0;
  int calvalusTokenLength = 0;
  while ((i = fread(calvalusToken+calvalusTokenLength, 1, sizeof calvalusToken, f)) > 0) 
     calvalusTokenLength += i;
  fclose(f);
  calvalusToken[calvalusTokenLength] = '\0';
  printf("calvalus token size=%d\n", calvalusTokenLength);
  printf("calvalus token=%s\n", calvalusToken);

  // split, decode, and decrpyt aes key
  
  char* p1 = strchr(calvalusToken, ' ');
  char* p2 = strchr(p1+1, ' ');
  *p1 = '\0';
  *p2 = '\0';
  int l1 = apr_base64_decode_len(calvalusToken);
  unsigned char iv[l1];
  l1 = apr_base64_decode((char*)iv, calvalusToken);
  int l2 = apr_base64_decode_len(p1+1);
  char aesrsa[l2];
  l2 = apr_base64_decode(aesrsa, p1+1);
  int l3 = apr_base64_decode_len(p2+1);
  char enchashandsaml[l3];
  l3 = apr_base64_decode(enchashandsaml, p2+1);

  printf("iv=%s\n", calvalusToken);
  printf("l1=%d\n", l1);
  printf("aesrsa=%s\n", p1+1);
  printf("l2=%d\n", l2);
  printf("hashandsaml=%s\n", p2+1);
  printf("l3=%d (%d)\n", l3, (int) strlen(p2+1));

  unsigned char aes[RSA_size(calvalus_priv_rsa)];
  int aeslen = RSA_private_decrypt(l2, (unsigned char*)aesrsa, aes, calvalus_priv_rsa, RSA_PKCS1_OAEP_PADDING);
  int e = ERR_get_error();
  printf("2 %s\n", ERR_error_string(e, NULL));
  printf("rsa_decrypt returns %d\n", aeslen);

  /* decrypt hash and saml */

  unsigned char hashAndSaml[strlen(p2+1)+1];
  int hashAndSamlLength;

  EVP_CIPHER_CTX ctx;
  EVP_CIPHER_CTX_init(&ctx);
  ERR_print_errors_fp(stderr);
  EVP_CIPHER_CTX_set_padding(&ctx, 1);

  int x = EVP_DecryptInit_ex(&ctx, EVP_aes_128_cbc(), NULL, aes, iv);
  ERR_print_errors_fp(stderr);
  x = EVP_DecryptUpdate(&ctx, hashAndSaml, &hashAndSamlLength, (unsigned char*)enchashandsaml, l3);
  ERR_print_errors_fp(stderr);
  int l4;
  x = EVP_DecryptFinal_ex(&ctx, hashAndSaml+hashAndSamlLength, &l4);
  ERR_print_errors_fp(stderr);
  hashAndSamlLength += l4;
  EVP_CIPHER_CTX_cleanup(&ctx);
  ERR_print_errors_fp(stderr);

  hashAndSaml[hashAndSamlLength] = '\0';
  char* p4 = strchr((char*) hashAndSaml, ' ');
  *p4 = '\0';
  int hashLen = apr_base64_decode_len((char*)hashAndSaml);
  unsigned char hash[hashLen];
  hashLen = apr_base64_decode((char*)hash, (char*)hashAndSaml);

  /* hash payload data */
  /* echo -n thisissomepayload|sha256sum */

  // only calvalus token is included in transfer for this test, not the payload
  char* payload = (char*) "thisissomepayload";

  unsigned char actualHash[EVP_MAX_MD_SIZE];
  int actualHashLen = do_hash(payload, actualHash);
  printhex(actualHash, actualHashLen);

  if (actualHashLen != hashLen || strncmp((char*) actualHash, (char*) hash, hashLen) != 0) {
    printf("hash differs");
    exit(1);
  }

  printf("hash matches\n");

  // let saml string survive method call
  char* result = malloc(strlen(p4+1)+1);
  strcpy(result, p4+1);
  return result;
}

//int main(int argc, char* argv[]) {
//  printf("%s\n", testDecrypt());
//}

