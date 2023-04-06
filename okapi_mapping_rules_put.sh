#!/bin/bash

OKAPIURL="http://FOLIO_URL:9130"
CURL="curl -w\n -D - "
PATH_TMP="working_dir"

H_TENANT="-HX-Okapi-Tenant:tenant"
H_JSON="-HContent-type:application/json"

echo "Ensure that Okapi can be reached ..."
STATUS=$(curl -s -S -w "%{http_code}" $OKAPIURL/_/proxy/tenants -o /dev/null)
if [ "$STATUS" != "200" ]; then
	  echo "Cannot contact okapi: $STATUS"
	    exit
fi
VERSION_OKAPI=$(curl -s -S "$OKAPIURL/_/version")
echo "Using version ${VERSION_OKAPI}"
echo

echo "Do login and obtain token ..."
cat >$PATH_TMP/login-credentials.json << END
{
  "username": "",
  "password": ""
}
END

$CURL $H_TENANT $H_JSON --progress-bar \
    -X POST \
    -d@$PATH_TMP/login-credentials.json \
    $OKAPIURL/authn/login > $PATH_TMP/login-response.json

echo "Extract the token header from the response ..."
H_TOKEN=-H$(grep -i x-okapi-token "$PATH_TMP/login-response.json" | sed 's/ //')
echo

echo $CURL
echo $H_TENANT
echo $H_TOKEN
echo "$OKAPIURL/mapping-rules"

echo "PUT marc-instance-mapping-rules"
$CURL $H_TENANT $H_TOKEN \
    -X PUT \
    -H "Content-type: application/json" \
    -d@$PATH_TMP/marc-instance-mapping-rules.json \
    "$OKAPIURL/mapping-rules/marc-bib"

echo
echo "[ Done ]"
