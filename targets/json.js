const etl = require('etl');

module.exports = function(stream,argv) {
  return stream.pipe(etl.stringify(0,null,true))
    .pipe(argv.source === 'screen' ? etl.map(d => console.log(d)) : etl.toFile(argv.dest));
};