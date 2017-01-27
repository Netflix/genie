var webpack = require("webpack");
var WebpackDevServer = require("webpack-dev-server");
var config = require("./webpack.config");

config.entry.app.unshift("webpack-dev-server/client?http://localhost:3000/");

new WebpackDevServer(webpack(config), {
  publicPath: config.output.publicPath,
  hot: false,
  historyApiFallback: true,
  proxy: { "*": "http://localhost:8080" }
}).listen(3000, "localhost", function(err, result) {
  if (err) console.log(err);

  console.log("Listening at localhost:3000");
});
