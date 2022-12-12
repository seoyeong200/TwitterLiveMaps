document.addEventListener("DOMContentLoaded", () => {
    var socket = io.connect()
    socket.on('event', function (value) {})

    for (let i = 0; i < 50; i++) {
        var num = Math.floor(Math.random()*10) % 2
      const accordion = document.createElement("details")
      
      if (num === 0) {
        accordion.innerHTML = "<summary> \
        <div class='lx-avatar'> \
        <img src='http://randomuser.me/api/portraits/women/" + i + ".jpg' alt='' /> \
        </div> \
        <p><b>Lorem ipsum</b><br/><b>Age:</b> i women </p>\
        </summary> \
        <h4>Bio:</h4> \
        <p>Lorem ipsum dolor sit amet consectetur adipisicing elit. Nemo libero nam ipsa voluptatem illo aspernatur, repellat dignissimos quo dolorem accusantium, tempora magni ipsam ut. Eum quidem delectus totam culpa placeat.</p>"
        document.querySelector("#women").append(accordion)
      }else {
        accordion.innerHTML = "<summary> \
        <div class='lx-avatar'> \
        <img src='http://randomuser.me/api/portraits/men/" + i + ".jpg' alt='' /> \
        </div> \
        <p><b>Lorem ipsum</b><br/><b>Age:</b> i men </p>\
        </summary> \
        <h4>Bio:</h4> \
        <p>Lorem ipsum dolor sit amet consectetur adipisicing elit. Nemo libero nam ipsa voluptatem illo aspernatur, repellat dignissimos quo dolorem accusantium, tempora magni ipsam ut. Eum quidem delectus totam culpa placeat.</p>"
        document.querySelector("#men").append(accordion)
      }

      
    }
  
  })