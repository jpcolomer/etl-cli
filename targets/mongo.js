const etl = require('etl');
const { EJSON } = require('bson');

module.exports = (stream,argv) => {
  const useEjson = argv.ejson && /false/i.exec(argv.ejson) ? false : true;
  ['target_uri','target_collection', 'target_indextype'].forEach(key => { if(!argv[key]) throw `${key} missing`;});
  const MongoClient = require('mongodb').MongoClient;
  const coll = new MongoClient(argv.target_uri).connect()
      .then(client => client.db(argv.target_collection).collection(argv.target_indextype));

  if (useEjson) stream = stream.pipe(etl.map(d => {
    return EJSON.parse(JSON.stringify(d));
  }));

  let out = stream.pipe(etl.collect(argv.collect || 10));

  if (!argv.update && !argv.upsert)
    return out.pipe(etl.mongo.insert(coll));
  else {
    let ids = (argv.upsert || argv.update);
    if (typeof ids === 'string')
      ids = ids.split(',');
    else
      ids = '_id';
    return out.pipe(etl.mongo.update(coll,ids,{upsert: !!argv.upsert}));
  }
};
