const { withEntitlementsPlist } = require("expo/config-plugins");

module.exports = function withLocalIosSigning(config) {
  return withEntitlementsPlist(config, (config) => {
    delete config.modResults["aps-environment"];
    delete config.modResults["com.apple.developer.applesignin"];
    return config;
  });
};
