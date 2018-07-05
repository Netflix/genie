var webpack = require("webpack");
var path = require("path");

var APP_DIR = path.resolve(__dirname, "src/main/web");
var BUILD_DIR = path.resolve(__dirname, "build/web/bundle");

var config = {
  entry: { app: [ APP_DIR + "/index.js" ] },
  output: { path: BUILD_DIR, filename: "bundle.js", publicPath: "/webjars/genie-ui/" },
  plugins: [
    new webpack.optimize.DedupePlugin(),
    new webpack.optimize.UglifyJsPlugin({ compressor: { warnings: false } }),
    new webpack.DefinePlugin({
      "process.env": { NODE_ENV: JSON.stringify("production") }
    })
  ],
  resolve: { extensions: [ "", ".js" ] },
  module: {
    loaders: [
      {
        test: /\.jsx?/,
        include: APP_DIR,
        exclude: /(node_modules|bower_components)/,
        loader: "babel-loader"
      },
      { test: /\.css$/, loader: "style-loader!css-loader" },
      {
        test: /\.(png|woff|woff2|eot|ttf|svg)$/,
        loader: "url-loader?limit=100000"
      },
      {
        test: /\.(ttf|svg|eot|woff|woff2)(\?v=[0-9]\.[0-9]\.[0-9])?$/,
        loader: "file-loader?name=[path][name].[ext]"
      }
    ]
  }
};

module.exports = config;
