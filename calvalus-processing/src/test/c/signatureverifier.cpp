#include <strings.h>
#include <openssl/evp.h>
#include <fstream>
#include <sstream>
#include <string>
#include <xercesc/dom/DOMDocument.hpp>
#include <xmltooling/XMLObject.h>
#include <xmltooling/XMLToolingConfig.h>
#include <xmltooling/util/ParserPool.h>
#include <xmltooling/security/BasicX509Credential.h>
#include <xmltooling/security/CredentialCriteria.h>
#include <xmltooling/security/CredentialResolver.h>
#include <xmltooling/validation/Validator.h>
#include <xmltooling/signature/Signature.h>
#include <xmltooling/signature/SignatureValidator.h>
#include <saml/signature/SignatureProfileValidator.h>
#include <saml/saml2/core/Assertions.h>
#include <saml/SAMLConfig.h>

using namespace xercesc;
using namespace xmltooling;
using namespace opensaml;
using namespace std;

extern "C" {
  char* testDecrypt();
  X509* read_cert(const char* filename);
}

void init() {
  SAMLConfig::getConfig().init();
}

saml2::Assertion* parseAssertion(string samlString) {
  istringstream in(samlString);
  DOMDocument* doc=XMLToolingConfig::getConfig().getParser().parse(in);
  XercesJanitor<DOMDocument> janitor(doc);
  const XMLObjectBuilder* b = XMLObjectBuilder::getBuilder(doc->getDocumentElement());
  XMLObject* xo=b->buildFromDocument(doc);
  janitor.release();
  saml2::Assertion* assertion = dynamic_cast<saml2::Assertion*>(xo);
  return assertion;
}

const Credential* readCredential(string resolverXmlFileName) {
  ifstream in(resolverXmlFileName.c_str());
  DOMDocument* doc=XMLToolingConfig::getConfig().getParser().parse(in);
  XercesJanitor<DOMDocument> janitor(doc);
  CredentialResolver* m_resolver = XMLToolingConfig::getConfig().CredentialResolverManager.newPlugin(
        FILESYSTEM_CREDENTIAL_RESOLVER,doc->getDocumentElement());
  CredentialCriteria cc;
  cc.setUsage(Credential::SIGNING_CREDENTIAL);
  Locker locker(m_resolver);
  const Credential* credential = m_resolver->resolve(&cc);
  return credential;
}

int main(int argc, char* argv[]) {

  init();

  // parse and unmarshal saml assertion
  string samlString(testDecrypt());
  //samlString.replace(samlString.find("testproject"),11,"operproject",0);
  cout << samlString << endl;
  saml2::Assertion* assertion = parseAssertion(samlString);
  cout << assertion->getIssuer()->getName() << endl;
  cout << assertion->getSignature() << endl;

  // read credentials using resolver and xml configuration
  const Credential* credential = readCredential("credentialresolver.xml");

  // validate structure
  SignatureProfileValidator spv;
  spv.validate(assertion->getSignature());
  cout << "structure valid" << endl;

  // validate against credential
  xmlsignature::SignatureValidator sv(credential);
  sv.validate(assertion->getSignature());
  cout << "signature valid" << endl;
}
