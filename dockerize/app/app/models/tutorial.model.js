// configure mongodb DB & mongoose
    // 3. define mongoose model(this represents tutorials collection in MongoDB database)
    // _id, title, description, published, createdAt, updatedAt, __v fields will be generated automatically for each Tutorial document
module.exports = mongoose => {
  var schema = mongoose.Schema(
    {
      title: String,
      description: String,
      published: Boolean
    },
    { timestamps: true }
  );

  // id instead of _id (to use this app with frontend) 
  // -> have to override toJSON method that map default object to a custom object
  schema.method("toJSON", function() {
    const { __v, _id, ...object } = this.toObject();
    object.id = _id; 
    return object;
  });
  // -> result will look like 
  // { "title": "Js Tut#", "description": "Description for Tut#", "published": true, "createdAt": "2020-02-02T02:59:31.198Z", "updatedAt": "2020-02-02T02:59:31.198Z", "id": "5e363b135036a835ac1a7da8"}
  const Tutorial = mongoose.model("tutorial", schema);

  return Tutorial;
};
