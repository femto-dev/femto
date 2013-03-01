/*
  (*) 2010-2013 Michael Ferguson <michaelferguson@acm.org>

    * This is a work of the United States Government and is not protected by
      copyright in the United States.

  This program is free software: you can redistribute it and/or modify
  it under the terms of the GNU Lesser General Public License as published by
  the Free Software Foundation, either version 3 of the License, or
  (at your option) any later version.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU Lesser General Public License for more details.

  You should have received a copy of the GNU Lesser General Public License
  along with this program.  If not, see <http://www.gnu.org/licenses/>.

  femto/src/mod_femto/femto-js/viz.js
*/

/*
 * TODO
 *  - reduce computation: make column drawing first compute selected tree
 *    before retrying/recursing.
 */


var REQUEST_NONE = 0;
var REQUEST_MATCHES = 1;
var REQUEST_CTX_BOTH = 2;
var REQUEST_CTX_LEFT = 3;
var REQUEST_CTX_RIGHT = 4;
var REQUEST_DOCS = 5;

var STATE_BLANK = 0;
var STATE_NEW_SEARCH = 1;
var STATE_UPDATING = 2;
var STATE_ERROR = -1;
var state = STATE_BLANK;

var docs_chunk_initial = 16;
var docs_chunk = 128;
var SIZE_THRESHOLD = 4096;

var offsets = 0;

var indexes = [ ];

// Matches is a hashtable of the elements like
// matches[string] = {
//  match: string key repeated here for convenience.
//  perindex:{"indexname":{range:[first,last], documents:{range:[first,last], docs: assoc array of doc_info -> {info:info, offsets:[ offsets ] }, requested }}},
//  cost:int,
//  count, // total count.
//  selected:bool,
//  left: assoc. array key ch, value 'node'
//  right: assoc. array key ch, value 'node'
// }
//
// a 'node' is of the form: (matches[string] is a 'node' too)
// { perindex:{"indexname":{range:[first,last], requested:true/false} },
//   match: string what the match at this point is
//   left or right: ch -> node array
// }
var query = "";
var matches = [ ];
var total_matches = 0;
var zoomed = { }; // define left=[char integers] or right=[char integers]

var requests_out = [ 0, 0, 0, 0, 0, 0 ];

var UPDATE_INITIAL_BOXES = 100;
var UPDATE_CHILD_BOXES_LEFT = 101;
var UPDATE_CHILD_BOXES_RIGHT = 102;
var UPDATE_DOCUMENTS = 103;

// containing { size:sz, kind:kind, obj:obj }
// if kind == STATE_INITIAL_BOXES, we're doing the 1st drawing on both sides
// otherwise, it's updating left/right boxes.
var to_update = [ ];
var to_update_needs_sort;

ascii_colors=new Array("red","turquoise","magenta","blue",
                       "green","yellow","cyan",
                       "orange","mediumpurple");
other_colors=new Array("darkred","darkturquoise","darkmagenta","darkblue",
                       "darkgreen","goldenrod","darkcyan",
                       "darkorange","purple");

var documents_grid;
var matches_grid;

var documents_columns = [
  {id:"info", name:"Info", field:"info", width:400, minWidth:50},
];

var matches_columns = [
  {id:"match", name:"Pattern", field:"match", width:220, minWidth:50},
  {id:"count", name:"# Matches", field:"count", width:100, minWidth:50},
  {id:"cost", name:"Cost", field:"cost", width:50, minWidth:50}
];
var slickoptions = {
  //forceFitColumns: true,
  //forceFitColumns: true
  //enableCellNavigation: false,
  //enableColumnReorder: true
  //asyncEditorLoading: true
};

var urlParams = { };

var svg;

if( !window.console ) console = { };
console.log = console.log || function(){};

function debug(string)
{
  console.log(string);
}

var more_requests_timeout = 100;
var more_display_timeout = 2000;

// tooltip support.
function pw()
{
  return window.innerWidth || document.documentElement.clientWidth || document.body.clientWidth;
}
function ph()
{
  return window.innerHeight || document.documentElement.clientHeight || document.body.clientHeight;
}
function mouseX(evt)
{
  return evt.clientX ? evt.clientX + (document.documentElement.scrollLeft || document.body.scrollLeft) : evt.pageX;
}
function mouseY(evt)
{
  return evt.clientY ? evt.clientY + (document.documentElement.scrollTop || document.body.scrollTop) : evt.pageY;
}

// expects top, left, width, height
//  returns new top,left
function offset_in_window(coords)
{
  var wndwidth = pw();
  var wndheight = ph();
  var left = coords.left;
  var top = coords.top;
  if( top < 0 ) {
    top = 0;
  }
  if( left < 0 ) {
    left = 0;
  }
  if( left + coords.width > wndwidth ) {
    left = wndwidth - coords.width;
  }
  if( top + coords.height > wndheight ) {
    top = wndheight - coords.height;
  }
  return {left:left, top:top};
}
function open_dialog_in_window(elt)
{
  var width = elt.dialog("option", "width");
  var height = elt.dialog("option", "height");
  var pos = elt.dialog("option", "position");
  if( pos[1] && ! (isNaN(pos[0]) || isNaN(pos[1]) ) ) {
    var off = offset_in_window({left:pos[0], top:pos[1],
                                width:width, height:height});
    elt.dialog({ position: [off.left, off.top] });
  }
  elt.dialog("open");
}

function color_for_ch(ch)
{
  if( ch >= 32 && ch < 126 ) {
    return ascii_colors[ch%ascii_colors.length];
  } else {
    return other_colors[Math.abs(ch)%other_colors.length];
  }
}

function new_search()
{
  state = STATE_NEW_SEARCH;
  matches = [ ];

  query = $( "#query" ).val();
  if( query == "" ) query = "''";

  zoomed = { };

  clear_work();

  $( "#tooltip" ).hide();
  $( "#tooltip" ).children().remove();

  display();
  display_documents();
  display_matches();
  $( "#query_matches" ).text("working...");
  $( "#moredocuments" ).button("enable");
}

function load_more_documents() {
  // Put the matches in our array..
  for( var m in matches ) {
    var v = matches[m];
    if( v.selected ) {
      state = STATE_UPDATING;
      add_work(v, UPDATE_DOCUMENTS);
    }
  }
  do_something();
}

function update_range(range, got)
{
  if( got[0] > got[1] ) return;

  if( got[0] < range[0] ) range[0] = got[0];
  if( got[1] > range[1] ) range[1] = got[1];
}

function request_error(response, textStatus, xhrReq)
{
  fail_error(textStatus);
}

var ctx_count = 0;

function do_request(index, request_type, request_obj, req)
{
  var ctx = {req:req, index:index, type:request_type, obj:request_obj, id:ctx_count};
  ctx_count++;

  function request_success(response, textStatus, xhrReq)
  {
    requests_out[ctx.type]--;

    switch (ctx.type) {
      case REQUEST_MATCHES:
        var m = response.matches;
        for( var i = 0; i < m.length; i++ ) {
          var v = m[i];
          var match = v.match;
          if( ! matches[match] ) {
            matches[match] = { match:match, perindex:[], cost: v.cost, selected:true };
          }
          matches[match].perindex[ctx.index] = {range:v.range};
        }

        // compute total_matches from matches
        total_matches = 0;
        for( var m in matches ) {
          var count = 0;
          var atm = matches[m];
          for( var i in atm.perindex ) {
            var pi = atm.perindex[i];
            count += 1 + pi.range[1] - pi.range[0];
          }
          atm.count = count;
          total_matches += count;
        }

        // Put the matches in our array..
        for( var m in matches ) {
          var v = matches[m];
          if( v.selected ) {
            add_work(v, UPDATE_INITIAL_BOXES);
            if( $( "#documents-dialog" ).dialog("isOpen") ) add_work(v, UPDATE_DOCUMENTS);
          }
        }

        state = STATE_UPDATING;
        display_matches(); // draw the matches.
        break;
      case REQUEST_CTX_BOTH:
        // ctx.obj is a matches[m].
        var left = response.left;
        var right = response.right;
        set_tree_attrs(ctx.index, left, ctx.obj, true, ctx.obj.match);
        set_tree_attrs(ctx.index, right, ctx.obj, false, ctx.obj.match);
        break;
      case REQUEST_CTX_LEFT:
        set_tree_attrs(ctx.index, response.left, ctx.obj, true, ctx.obj.match);
        break;
      case REQUEST_CTX_RIGHT:
        set_tree_attrs(ctx.index, response.right, ctx.obj, false, ctx.obj.match);
        break;
      case REQUEST_DOCS:
        // ctx.obj is a matches[m]
        if( ! ctx.obj.perindex[ctx.index].documents ) {
          ctx.obj.perindex[ctx.index].documents = { docs: [ ] };
        }
        var docs = ctx.obj.perindex[ctx.index].documents;
        if( docs.range ) update_range(docs.range, response.range);
        else docs.range = response.range;

        for( var i=0; i < response.results.length; i++ ) {
          var v = response.results[i];
          if( docs.docs[v.doc_info] == undefined ) {
            docs.docs[v.doc_info] = { info: v.doc_info, offsets: [ ] };
          }
          if( v.offsets ) {
            for( var j=0; j < v.offsets.length; j++ ) {
              docs.docs[v.doc_info].offsets.push(v.offsets[j]);
            }
          }
        }

        display_documents();
        break; 
    }


    switch (ctx.type) {
      case REQUEST_MATCHES:
      case REQUEST_CTX_BOTH:
      case REQUEST_CTX_LEFT:
      case REQUEST_CTX_RIGHT:
        // redisplay everything... in a moment,
        // allowing other requests to be done.
        display();
        break;
    }

    // do something in a moment... (to get a chance to be responsive).
    window.setTimeout( do_something, more_requests_timeout );
  }


  requests_out[ctx.type]++;

  var req_data = encodeURIComponent(ctx.req);

  var request = jQuery.ajax({ type: 'GET',
                              // ask for e.g. /femto/index_1/r
                              url: ctx.index + "r",
                              data: req_data,
                              success: request_success,
                              error: request_error,
                              dataType: 'json' });

  if( ! request ) {
    fail_error("Could not get jQuery.ajax");
  }
}

function add_work(obj, st)
{
  to_update.push( {size:obj.count, kind:st, obj:obj} );
  to_update_needs_sort = true;
}
function get_work()
{
  if( to_update_needs_sort ) {
    to_update_needs_sort = false;
    to_update.sort(function(a,b){return a.size - b.size;});
  }
  return to_update.pop();
}
function has_work()
{
  return to_update.length > 0;
}
function clear_work()
{
  to_update = [ ];
}

function do_something_internal()
{
  switch (state) {
    case STATE_BLANK:
      // Do nothing until a search is submitted.
      break;
    case STATE_NEW_SEARCH:
      matches = [ ];

      // Start a new search.
      for( var i=0; i < indexes.length; i++ ) {
        do_request(indexes[i], REQUEST_MATCHES, matches, "find_strings " + query);
      }
      return true;
    case STATE_UPDATING:
      var totrequests = 0; 
      // get something from to_update and work on that.
      while ( totrequests < 3 && has_work() ) {
        var work = get_work();
        var obj = work.obj;
        switch (work.kind) {
          case UPDATE_INITIAL_BOXES:
            var nrequests = 0;
            // obj is matches[string]
            for( var index in obj.perindex ) {
              var pi = obj.perindex[index];
              if( ! pi.requested ) {
                do_request(index, REQUEST_CTX_BOTH, obj, "string_rows_all " + parse_pattern(obj.match).join(" "));
                pi.requested = true;
                nrequests++;
              }
            }
            if( nrequests > 0 ) $( "#spinner" ).show( );
            totrequests += nrequests;
            return true;
          case UPDATE_DOCUMENTS:
            var nrequests = 0;
            // object is matches[string]
            for( var index in obj.perindex ) {
              var pi = obj.perindex[index];
              var start = -1;
              var end = -2;
              var chunk = docs_chunk_initial;
              start = pi.range[0];
              if( pi.documents ) {
                // We've already got some... move on to the chunk
                // after what we already have.
                start = pi.documents.range[1] + 1;
                chunk = docs_chunk;
              }
              end = pi.range[1];
              if( start <= end ) {
                do_request(index, REQUEST_DOCS, obj, "docs_for_range " + chunk + " " + offsets + " " + start + " " + end );
                nrequests++;
              }
            }
            if( nrequests > 0 ) $( "#docspinner" ).show( );
            totrequests += nrequests;
            return true;
          case UPDATE_CHILD_BOXES_LEFT:
          case UPDATE_CHILD_BOXES_RIGHT:
            var nrequests = 0;
            if( obj.visible_size && obj.visible_size > SIZE_THRESHOLD && ! (obj.left || obj.right ) ) {
              for( var index in obj.perindex ) {
                var pi = obj.perindex[index];
                if( ! pi.requested ) {
                  if( work.kind == UPDATE_CHILD_BOXES_LEFT ) {
                    do_request(index, REQUEST_CTX_LEFT, obj, "string_rows_left " + parse_pattern(obj.match).join(" "));
                  } else {
                    do_request(index, REQUEST_CTX_RIGHT, obj, "string_rows_right " + parse_pattern(obj.match).join(" "));
                  }
                  nrequests++;
                  pi.requested = true;
                }
              }
            }
            if( nrequests > 0 ) $( "#spinner" ).show( );
            totrequests += nrequests;
            return true;
        }
      }
      if( totrequests > 0 ) return true;
      break;
    case STATE_ERROR: break; // do nothing!
  }
  return false;
}

var in_do_something = false;
function do_something() {
  if( in_do_something ) return;
  in_do_something = true;
  do_something_internal();
  in_do_something = false;
}

function fail_error(error)
{
  state = STATE_ERROR;

  $( "#query_matches" ).text("an error occurred: " + error);
}

function set_tree_attrs(index, arr, obj, isleft, pat)
{
  for( var i = 0; i < arr.length; i++ ) {
    var v = arr[i];
    var child;
    if( isleft ) {
      if( ! obj.left ) {
        obj.left = [ ];
      }
      child = obj.left;
    } else {
      if( ! obj.right ) {
        obj.right = [ ];
      }
      child = obj.right;
    }
    if( ! child[v.ch] ) {
      var crumbs;
      if( isleft ) {
        crumbs = get_char(v.ch)+pat;
      } else {
        crumbs = pat + get_char(v.ch);
      }
      child[v.ch] = { perindex:[ ], match: crumbs };
    }
    child[v.ch].perindex[index] = {range:v.range};
  }
}

function get_char(code)
{
  var use_hex = 0;
  var use_back = 0;
 
  code = Math.floor(code); // make sure it's an integer!

  if( code < 32 ) {
    use_hex = 1;
  }
  if( code > 126 ) {
    use_hex = 1;
  }
  if( code == "\"".charCodeAt(0) ) {
    use_back = 1;
  }
  
  if( use_hex ) {
    var hex = code.toString(16);
    if( hex.length == 1 ) hex = "0" + hex;
    return "\\x" + hex;
  } else if( use_back ) {
    return "\\" + String.fromCharCode(code);
  } else {
    return String.fromCharCode(code);
  }
}

function match_to_str(pat)
{
  var ret = "";
  for( var i=0; i < pat.length; i++ ) {
    ret = ret + get_char(pat[i]);
  }
  return ret;
}

function parse_pattern(str)
{
  var ret = [];
  for( var i = 0; i < str.length; i++ ) {
    if( str.substring(i,i+2) == "\\x" || str.substring(i,i+2) == "\\X" ) {
      i++; // pass \
      i++; // pass x
      var hex = str.substring(i, i+2);
      i++; // pass 1st of 2 chars.
      var int = parseInt(hex, 16);
      if( isNaN(int) ) {
      } else {
        ret.push(int);
      }
    } else {
      ret.push(str.charCodeAt(i));
    }
  }
  return ret;
}

function pattern_to_query(pat)
{
  return "'" + match_to_str(pat) + "'";
}

// makes a 100x100 box. from 0 to 100
function make_box(color, text, subtext, count_text, v, pat, isleft, showtext, showsubtext)
{
  var pattern = match_to_str(pat);
  var crumb;
  if( isleft ) crumb = selected_matches_string(pattern, "");
  else crumb = selected_matches_string("", pattern);
  var box_context = { text:text, subtext:subtext, count:count_text, v:v, pat:pat.slice(0), crumb:crumb, isleft:isleft, inbox:false, intip:false };

  function close_tooltip() {
    $( "#tooltip" ).hide();
    $( "#tooltip" ).children().remove();
    box_context.inbox = false;
    box_context.intip = false;
  }

  function box_click() {
    close_tooltip();

    if( box_context.isleft ) zoomed.left = box_context.pat;
    else zoomed.right = box_context.pat;

    var wnd = window_size();
    var sz = wnd.width * wnd.height;
    for( var i=0; i < box_context.v.nodes.length; i++ ) {
      var node = box_context.v.nodes[i];
      node.visible_size = sz;
      if( box_context.isleft ) {
        if( ! node.left ) add_work(node, UPDATE_CHILD_BOXES_LEFT);
      } else {
        if( ! node.right ) add_work(node, UPDATE_CHILD_BOXES_RIGHT);
      }
    }
    display();
    do_something();
  }
  function maybe_close_tooltip()
  {
    if( ! (box_context.inbox || box_context.intip) ) {
      close_tooltip();
    }
  }
  function box_mouseover(evt) {
    var x;
    var y;
    function onmousemove(evt) {
      if( box_context.inbox  ) {
        x = mouseX(evt);
        y = mouseY(evt);
      } else {
        document.onmousemove = null;
      }
    }
    function tooltip_mousein(evt) {
      box_context.intip = true;
    }
    function tooltip_mouseout(evt) {
      box_context.intip = false;
      window.setTimeout(maybe_close_tooltip, 100);
    }

    function display_tooltip() {
      if( box_context.inbox ) {
        $( "#tooltip" ).children().remove();
        $( "#tooltip" ).append("<h1>" + box_context.text + "</h1>" +
                                "<h2>" + box_context.crumb + "</h2>" +
                                "<p>" + box_context.subtext + " (" + box_context.count + " matches) </p>" );
        var width = $( "#tooltip" ).outerWidth(true);
        var height = $( "#tooltip" ).outerHeight(true);
        //$( "#tooltip" ).position({ top: y, left: x });
        var coords = offset_in_window({top: y + 10, left: x + 10,
                                       width: width, height: height});
        $( "#tooltip" ).css('left', coords.left);
        $( "#tooltip" ).css('top', coords.top);
        $( "#tooltip" ).mouseenter( tooltip_mousein );
        $( "#tooltip" ).mouseleave( tooltip_mouseout );
        $( "#tooltip" ).show();
      }
    }

    box_context.inbox = true;

    document.onmousemove = onmousemove;

    window.setTimeout( display_tooltip, 800 );
  }
  function box_mouseout(evt) {
    box_context.inbox = false;
    window.setTimeout(maybe_close_tooltip, 100);
  }
  var g = svg.group();
  var subg = svg.group(g, {transform:"translate(50,50)"});
  var r = svg.rect(subg, -50, -50, 100, 100, {fill: color});

  g.addEventListener("click",box_click,false);
  g.addEventListener("mouseover",box_mouseover,false);
  g.addEventListener("mouseout",box_mouseout,false);

  //r.click( function(e){ alert("clicked"); } );

  var t;
  if( showtext && text != "" ) {
    t = svg.text(subg, text, {class_:"femtotext",
                              fontSize:"48", 
                              textLength:"98",
                              lengthAdjust:"spacing"});
    //t.addEventListener("click",box_click,false);
  }
  var s;
  if( showsubtext && subtext != "" ) {
    s = svg.text(subg, 0, 40, subtext, {class_:"femtosubtext",
                                        width:"100", 
                                        height:"10",
                                        fontSize:"10"} );
    //s.addEventListener("click",box_click,false);
  }

  return g;
}

// We center, left or right justify.
function make_box_at(rect, color, text, subtext, count_text, rightjustify, size_only, v, pat)
{
  var width = rect.right - rect.left;
  var height = rect.bottom - rect.top;
  var scalex = width / 100.0;
  var scaley = height / 100.0;
  var scale = scalex;
  if( scaley < scalex ) scale = scaley;
  var box_size = 100.0 * scale;
  var half_box = box_size / 2.0;
  var show_text = true;
  var show_subtext = true;
  if( box_size < 32 ) show_subtext = false;
  if( box_size < 16 ) show_text = false;
  var xtrans = rect.left;
  if( rightjustify ) {
    xtrans = rect.right - 100.0*scale;
  }
  var ytrans = rect.top + (rect.bottom - rect.top) / 2.0 - half_box;

  //xtrans = Math.floor(xtrans);
  //ytrans = Math.floor(ytrans);

  var b = undefined;
  if( ! size_only ) {
    b = make_box(color, text, subtext, count_text, v, pat, rightjustify, show_text, show_subtext);

    var xform = "translate(" + xtrans + "," + ytrans + ") scale(" + scale + ")";
    svg.change(b, {transform:xform});
  }

  var ret = {svg: b,
             outrect: {left: (xtrans),
                       right:(xtrans + box_size),
                       top: (ytrans),
                       bottom: (ytrans + box_size)} };
/*
             outrect: {left: Math.floor(xtrans),
                       right: Math.ceil(xtrans + box_size),
                       top: Math.floor(ytrans),
                       bottom: Math.ceil(ytrans + box_size)} };
*/
  return ret;
}

function window_size()
{
  var w = 0;
  var h = 0;
  if( self.innerHeight ) {
    w = self.innerWidth;
    h = self.innerHeight;
  } else if( document.documentElement && document.documentElement.clientHeight ) {
    w = document.documentElement.clientWidth;
    h = document.documentElement.clientHeight;
  } else if( document.body ) {
    w = document.body.clientWidth;
    h = document.body.clientHeight;
  }

  return {width: w, height: h};
}

function max_depth(arr)
{
  var ret = 1;
  for( var i = 0; i < arr.length; i++ ) {
    if( arr[i].child ) {
      var depth = 1 + max_depth(arr[i].child);
      if( depth > ret ) ret = depth;
    }
  }
  return ret;
}

// arr is of the form:
// [ {a:{node...}, b:{node}}, {a:{node...}, b:{node..}} ]
function make_column_internal(rect, arr, rightjustify, basewidth, size_only, pat_in)
{
  // swap the input to create nodes per input.
  var bych = [ ];
  for(var i=0; i < arr.length; i++) {
    var eachbych = arr[i];
    for(var ch in eachbych) {
      var node = eachbych[ch];
      if( ! bych[ch] ) {
        bych[ch] = {count:0, nodes:[], children:[]};
      }
      var v = bych[ch];

      // add a link to this one
      v.nodes.push(node);

      // add child nodes.
      if( node.left ) v.children.push(node.left);
      else if( node.right ) v.children.push(node.right);

      // sum up the count.
      var count = 0;
      for( var index in node.perindex ) {
        var pi = node.perindex[index];
        count += 1 + pi.range[1] - pi.range[0];
      }
      v.count += count;
    }
  }
  // remove any partial child nodes.
  for( var ch in bych ) {
    var v = bych[ch];
    if( v.children.length < arr.length ) {
      delete v.children;
    }
  }

  var keys = [ ];
  for( var ch in bych ) {
    keys.push(Math.floor(ch));
  }
  // sort keys numerically.
  keys.sort(function(a,b){return a - b;});

  var height = rect.bottom - rect.top;

  //debug("starting make column " + rect.left);

  // first, compute the total number of rows in arr.
  var tot = 0;
  var totcount = 0;
  var max_scale = undefined;
  var num_nonzero = 0;
  for( var ch in bych ) {
    var v = bych[ch];
    totcount += v.count;
  }
  for( var ch in bych ) {
    var v = bych[ch];
    tot += scale_size(v.count, totcount); 
  }

  // Now compute the size of each box.
  for( var ch in bych ) {
    var v = bych[ch];
    v.scale = scale_size(v.count, totcount) / tot;
    if( max_scale == undefined || v.scale > max_scale ) {
      max_scale = v.scale;
    }
    if( v.scale > 0.0 ) {
      num_nonzero++;
    }
  }

  var scaleall = 1.0;

  // Imagine drawing a box with max_scale size.
  if( max_scale != undefined ) {
    var cur = 0.0;
    var scale = max_scale;
    var t = Math.floor(rect.top + cur*height);
    var b = Math.ceil(t + scale*height);
    var trec = {left: rect.left, right: rect.right,
                top: t, bottom: b};
    if( rightjustify ) {
      trec.left = trec.right - basewidth;
    } else {
      trec.right = trec.left + basewidth;
    }


    var b = make_box_at(trec, color_for_ch(0), get_char(0), "100%", "1", rightjustify, true, v);
    var wantheight = trec.bottom - trec.top;
    var gotheight = b.outrect.bottom - b.outrect.top;
    if( gotheight < wantheight ) {
      scaleall = (gotheight/wantheight);
    }
  }

  //debug("scaleall " + scaleall);

  var max_bot = undefined;
  var max_right = undefined;
  var min_left = undefined;

  var pat;
  // Make a new pattern array with space for a new character.
  if( rightjustify ) pat = [ -10 ].concat(pat_in);
  else pat = pat_in.concat( [ -10 ] );


  // Now create boxes that are scaled and transformed.
  // In the first pass, we just see how big everything is,
  // so that we can center.
  // In the second, we store it all.
  for( var pass = 0; pass < 2; pass++ ) {
    var cur = 0.0;
    var add_top = 0;

    var do_size_only = size_only;
    if( pass == 0 ) {
      do_size_only = true;
    } else {
      if( max_bot < height ) add_top = (height - max_bot) / 2.0;
    }

    for( var ki=0; ki<keys.length; ki++ ) {
      ch = keys[ki];
      var v = bych[ch];

      // set the character in pat.
      if( rightjustify ) pat[0] = parseInt(ch);
      else pat[pat.length - 1] = parseInt(ch);

      var percent = (100.0 * v.count) / totcount;
      percent = String(percent).substr(0,4);
      percent = percent + "%";
      var scale = v.scale;
      var t = Math.floor(rect.top + cur*height) + add_top;
      var b = Math.ceil(t + scale*height*scaleall);
      var trec = {left: rect.left, right: rect.right,
                  top: t, bottom: b};
      if( rightjustify ) {
        trec.left = trec.right - basewidth;
      } else {
        trec.right = trec.left + basewidth;
      }

      if( v.count > 0 ) {
        var b = make_box_at(trec, color_for_ch(ch), get_char(ch), percent, v.count, rightjustify, do_size_only, v, pat);
        var orec = b.outrect;

        if( max_bot == undefined || orec.bottom > max_bot ) max_bot = orec.bottom;

        var visible_size = (orec.right - orec.left) * (orec.bottom - orec.top);
        if( orec.bottom < rect.top ||
            orec.right < rect.left ||
            orec.top > rect.bottom ||
            orec.left > rect.right ) visible_size = 0;

        // set visible_size of all linked nodes....
        if( ! size_only ) {
          for( var i = 0; i < v.nodes.length; i++ ) {
            var node = v.nodes[i];
            node.visible_size = visible_size;
            if ( node.left || node.right ) {
              // OK, we already have a child.
            } else if( visible_size > SIZE_THRESHOLD ) {
              // No children. Add some work.
              if( rightjustify ) add_work(node, UPDATE_CHILD_BOXES_LEFT);
              else add_work(node, UPDATE_CHILD_BOXES_RIGHT);
            }
          }
        }

        if( max_right == undefined || orec.right > max_right ) {
          max_right = orec.right;
        }
        if( min_left == undefined || orec.left < min_left ) {
          min_left = orec.left;
        }


        // If there is a child, add the child node.
        if( v.children ) {
          var pad = 2;
          var crect = {left: trec.left, right: trec.right,
                       top: orec.top + pad, bottom: orec.bottom - pad};
          var width = crect.right - crect.left;
          if( rightjustify ) {
            crect.right = orec.left - pad;
            crect.left = crect.right - width;
          } else {
            crect.left = orec.right + pad;
            crect.right = crect.left + width;
          }
          if( crect.left < crect.right && crect.top < crect.bottom ) {
            var child = make_column_internal(crect, v.children, rightjustify, basewidth, do_size_only, pat);
            orec = child.outrect;
            //debug("child orec left " + orec.left);
          }
        }

        if( max_right == undefined || orec.right > max_right ) {
          max_right = orec.right;
        }
        if( min_left == undefined || orec.left < min_left ) {
          min_left = orec.left;
        }

      }


      cur = cur + scale;
    }
  }

  var orec = {left: rect.left, right: rect.right,
              top: rect.top, bottom: rect.bottom};
  if( max_right != undefined ) {
    orec.right = max_right;
  }
  if( min_left != undefined ) {
    orec.left = min_left;
  }

  //debug("max right " + max_right + " min left " + min_left);

  var ret = {outrect: {left: orec.left,
                       right: orec.right,
                       top: orec.top,
                       bottom: orec.bottom } };
  //debug("finishing make column " + rect.left + "new left is " + orec.left);
  return ret;
}

function make_column(rect, arr, rightjustify, pat)
{
  var wantwidth = rect.right - rect.left;
  var first = true;
  var multiple = 1;
  var gotwidth;
  var basewidth;
  do {
    basewidth = Math.floor(wantwidth / multiple);
    var ret = make_column_internal(rect, arr, rightjustify, basewidth, true, pat);
    gotwidth = ret.outrect.right - ret.outrect.left;
    //debug("tried multiple " + multiple + " got width " + gotwidth + " want width " + wantwidth);
    if( gotwidth > wantwidth ) {
      if( first ) {
        var div = gotwidth / wantwidth;
        //debug("div is " + div);
        multiple = Math.ceil(gotwidth / wantwidth);
      } else {
        multiple += 2*Math.ceil(gotwidth / wantwidth);
      }
    }
    first = false;
  } while( gotwidth > wantwidth );

  //debug("gotwidth " + gotwidth + " wantwidth " + wantwidth + " multiple " + multiple + " basewidth " + basewidth);

  return make_column_internal(rect, arr, rightjustify, basewidth, false, pat);
}

function n_matches(x)
{
  var ret = x.range[1] - x.range[0] + 1;
  if( ret < 0 ) {
    return 0;
  } else {
    return ret;
  }
}

function scale_size(m, totcount)
{
  if( m == 0 ) {
    return 0; // leave a gap!
  } else {
    if( totcount == 0 ) return Math.sqrt(m);
    else return Math.sqrt(m / totcount);
  }
}

// Given something of the form
// [ { ch0:..., ch1:...}, {ch0:...,  ch1, ...} ]
// we merge it by ch and set counts.
function tree_matches(list)
{
  var chmap = { };
  for(var i=0; i<list.length; i++ ) {
    var l2 = list[i];
    for( var ch in l2 ) {
      var rec = l2[j];
      var ch = rec.ch;
      var count = n_matches(rec);
      var child = rec.child;
      if( chmap[ch] == undefined ) {
        chmap[ch] = {ch:ch, count:0, tomerge:[], matches:[]};
      }
      var to = chmap[ch];
      to.count += count;
      // save the child as something we need to merge.
      to.tomerge.push(child);
      matches.push(rec);
    }
  }

  var chrs = [];
  // OK, now merge the saved-up-work in tomerge and return
  // the final result.
  for (var k in chmap) {
    var v = chmap[k];

    if( v.tomerge.length == list.length ) {
      // Don't do anything if we have partial results here.
      v.child = tree_matches(v.tomerge);
    }
    delete chmap[k].tomerge;
    chrs.push(k);
  }
  
  // sort numerically by character.
  chrs.sort(function(a,b){return a-b;});

  // OK, now turn it all into an array that the rest
  // of the code is used to dealing with.
  var ret = [ ];
  for (var i=0; i < chrs.length; i++ ) {
    ret.push(chmap[chrs[i]]);
  }

  return ret;
}

function generate_tree()
{
  var lefts = [ ];
  var rights = [ ];

  for(var str in matches) {
    if( matches[str].selected ) {
      if( matches[str].left ) lefts.push(matches[str].left);
      if( matches[str].right ) rights.push(matches[str].right);
    }
  }

  var ret = { };
  if( lefts.length > 0 ) ret.left = tree_matches(lefts);
  if( rights.length > 0 ) ret.right = tree_matches(rights);

  return ret;
}

function display_documents()
{

  if( ! $( "#documents-dialog" ).dialog("isOpen") ) return;
 
  var new_documents = [ ];
  var total_got = 0;
  var total_avail = 0;

  for( var m in matches ) {
    var match = matches[m];
    if( match.selected ) {
      for( var index in match.perindex ) {
        var pi = match.perindex[index];
        total_avail += 1 + pi.range[1] - pi.range[0];
        if( pi.documents ) {
          total_got += 1 + pi.documents.range[1] - pi.documents.range[0];
          for( var doc in pi.documents.docs ) {
            new_documents.push(pi.documents.docs[doc]);
          }
        }
      }
    }
  }

  if( total_avail > 0 ) {
    var percent = (100.0 * total_got) / total_avail;
    percent = String(percent).substr(0,4);
    percent = percent + "%";
    $( "#docprogress" ).text( percent + " reporting" );
  } else {
    $( "#docprogress" ).text( "" );
  }

  if( requests_out[REQUEST_DOCS] == 0 ) {
    $( "#docspinner" ).hide( );
    $( "#moredocuments" ).button("enable");
  }

  documents_grid.removeAllRows();
  documents_grid.setData( new_documents );
  documents_grid.updateRowCount();
  documents_grid.setSelectedRows( [ ] );
  documents_grid.render();
}

// onSelectedRowsChanged handler
function select_matches()
{
  var rows = matches_grid.getSelectedRows();
  var data = matches_grid.getData();

  for( var m in matches ) {
    matches[m].selected = false;
  }
  for(var i=0; i < rows.length; i++ ) {
    var item = data[rows[i]];
    item.selected = true;
  }

  display_documents();
  display();
  do_something();
}


function display_matches()
{
  var new_matches = [ ];
  var selected = [ ];
  var i=0;
  for( var m in matches ) {
    new_matches.push(matches[m]);
    if( matches[m].selected ) selected.push(i);
    i++;
  }

  matches_grid.setData( new_matches );
  matches_grid.updateRowCount();
  matches_grid.setSelectedRows( selected );
  matches_grid.render();

  // How many matches do we have?
  $( "#query_matches" ).text(total_matches + " matches");
}

// before_text and after_text should not have quotes.
function selected_matches_string(before_text, after_text)
{
  var arr = [ ];
  for(var m in matches) {
    if( matches[m].selected ) {
      arr.push(m);
    }
  }

  var text;
  if( arr.length == 1 ) {
    text = "\"" + before_text + arr[0] + after_text + "\"";
  } else {
    text = "";
    for(var i=0; i < arr.length; i++ ) {
      if( text != "" ) text += "|"
      text += "\"" + arr[i] + "\"";
    }
    text = "(" + text + ")"
    if( text.length > 30 ) text = " (selected matches) ";
    var qbefore;
    var qafter;
    if( before_text == "" ) qbefore = "";
    else qbefore = "\"" + before_text + "\" ";
    if( after_text == "" ) qafter = "";
    else qafter = " \"" + after_text + "\"";

    text = qbefore + text + qafter;
  }

  return text;
}

var needs_display = false;

// Display our SVG drawing.
function display_internal()
{
  needs_display = false;

  // First, remove any SVG we've drawn.
  svg.clear();
 
  if( requests_out[REQUEST_CTX_BOTH] == 0 &&
      requests_out[REQUEST_CTX_LEFT] == 0 &&
      requests_out[REQUEST_CTX_RIGHT] == 0 ) {
    $( "#spinner" ).hide( );
  }

  // make sure the svg canvas is the right size.
  //svg.configure({width: "100%", height: "100%"}, false);
  svg.configure({width: "100%", height: "100%"}, false);
 
  var wnd = window_size();
  var canvasheight = $( "#svgcanvas" ).innerHeight();
  var canvaswidth = $( "#svgcanvas" ).innerWidth();

  // subtract the height of the navigation bar.
  canvasheight -= $( "#navigation-bar" ).outerHeight();

  // Add some room.
  var canvastop = 5;
  canvasheight -= 10;

  $( "#matches-list" ).children().remove();
  $( "#matches-list" ).hide();

  if( zoomed.left || zoomed.right ) {
    var text;

    if( zoomed.left ) text = selected_matches_string(match_to_str(zoomed.left), "");
    if( zoomed.right ) text = selected_matches_string("", match_to_str(zoomed.right));

    $( "#viz-breadcrumbs" ).show();
    //$( "#viz-breadcrumbs-label" ).text("Showing subtree for ");
    $( "#viz-breadcrumbs-match" ).text(text);

    var rect = { left: 0,
                 right: canvaswidth,
                 top: canvastop,
                 bottom: canvasheight };

    var sz = wnd.width * wnd.height;
    // First, merge whatever we have from matches.
    var arr = [ ];
    var ok = true;
    var isleft = (zoomed.left != undefined);
    for(var str in matches) {
      var match = matches[str];
      if( match.selected ) {
        var node = match;
        var zoomto;
        if( isleft ) {
          zoomto = zoomed.left;

          for( var i = zoomto.length - 1; i >=0 && node; i-- ) {
            node.visible_size = sz;
            node = node.left[zoomto[i]];
          }
        } else {
          zoomto = zoomed.right;

          for( var i = 0; i < zoomto.length && node; i++ ) {
            node.visible_size = sz;
            node = node.right[zoomto[i]];
          }
        }
        if( node ) {
          if( isleft ) {
            if( node.left ) arr.push(node.left);
            else {
              node.visible_size = sz;
              add_work(node, UPDATE_CHILD_BOXES_LEFT);
              ok = false;
            }
          } else {
            if( node.right ) arr.push(node.right);
            else {
              node.visible_size = sz;
              add_work(node, UPDATE_CHILD_BOXES_RIGHT);
              ok = false;
            }
          }
        } else {
          ok = false;
        }

        if( !ok ) break;
      }
    }

    // Now draw the boxes.
    if( arr.length > 0 && ok ) {
      var col = make_column(rect, arr, isleft, zoomto);
    }

  } else {
    $( "#viz-breadcrumbs" ).hide();
    // Draw the matches.
    $( "#matches-list" ).show();
   
    for(var m in matches) {
      if( matches[m].selected ) {
        $( "#matches-list" ).append( "<p class=\"femtomatch\">" + m + "</p>\n" );
      }
    }

    var matches_width = $( "#matches-list" ).outerWidth();
    var matches_height = $( "#matches-list" ).outerHeight();
    var matches_top = canvastop + Math.floor(canvasheight/2-matches_height/2);
    var matches_left = Math.floor(canvaswidth/2-matches_width/2);
    $( "#matches-list" ).offset({top: matches_top, left:matches_left});
    $( "#matches-list" ).show();

    var rightrect = { left: matches_left + matches_width + 10,
                      right: canvaswidth,
                      top: canvastop,
                      bottom: canvasheight };
    var leftrect = { left: 0,
                     right: matches_left - 10,
                     top: canvastop,
                     bottom: canvasheight };

    for(var m in matches) {
      matches[m].visible_size = matches_height * matches_width;
    }

    // First, merge whatever we have from matches.
    var lefts = [ ];
    var rights = [ ];
    var leftok = true;
    var rightok = true;
    for(var str in matches) {
      if( matches[str].selected ) {
        if( matches[str].left ) lefts.push(matches[str].left);
        else leftok = false;
        if( matches[str].right ) rights.push(matches[str].right);
        else rightok = false;
      }
    }

    // Now draw the boxes.
    if( lefts.length > 0 && leftok ) {
      var col = make_column(leftrect, lefts, 1, [ ]);
    }
    if( rights.length > 0 && rightok ) {
      var col = make_column(rightrect, rights, 0, [ ]);
    }
  }

}

var in_display = false;
function display()
{
  needs_display = true;
  if( ! in_display ) {
    in_display = true;
    for( var i=0; needs_display; i++ ) {
      needs_display = false;
      if( i == 0 ) display_internal();
      else window.setTimeout(display_internal, more_display_timeout);
    }
    in_display = false;
  }
}


function sgwidth(sg) {
  var cols = sg.getColumns();
  var width = 0;
  for( var i = 0; i < cols.length; i++ ) {
    width += cols[i].width;
  }
  return width + 25;
}

function resize_handler(grid)
{
  return function(event, ui) {
    grid.autosizeColumns();
    /*var grid_width = sgwidth(grid);
    var dialog_width = ui.dialog("width");

    if( grid_width < dialog_width ) {
    }
    var cols = grid.getColumns();
    var width = 0;
    for( var i = 0; i < cols.length; i++ ) {
      width += cols[i].width;
    }
    width += 25;
    grid.resizeCanvas();*/
  }
}


// The jQuery 'on page load' function
$(function() {

  // Get url parameters.
  {
    var e,
        a = /\+/g,  // Regex for replacing addition symbol with a space
        r = /([^&;=]+)=?([^&;]*)/g,
        d = function (s) { return decodeURIComponent(s.replace(a, " ")); },
        q = window.location.search.substring(1);

    while (e = r.exec(q))
      urlParams[d(e[1])] = d(e[2]);
  }

  if( urlParams.pattern != undefined ) {
    $( '#query' ).val(pattern_to_query(parse_pattern(urlParams.pattern)));
  } else if( urlParams.query != undefined ) {
    $( '#query' ).val(urlParams.query);
  }

  // Set the index path.
  if( urlParams.index != undefined ) {
    if( urlParams.index.substr(0,1) == "\"" ||
        urlParams.index.substr(0,1) == "[" ) {
      var ok = false;
      var err = "";
      try {
        var got = jQuery.parseJSON(urlParams.index);
        if( got instanceof Array ) {
          indexes = got;
          ok = true;
        } else if( got instanceof String ) {
          indexes = [ got ];
          ok = true;
        } else {
          err = "Unknown type";
          ok = false;
        }
      } catch(err) {
        err = err.description;
        ok = false;
      }
      if( ! ok ) alert("Error parsing index GET argument: " + err);
    } else {
      indexes = [ urlParams.index ];
    }
    var indexes_text;
    if( indexes.length == 1 ) {
      indexes_text = indexes[0];
    } else {
      indexes_text = JSON.stringify(indexes);
    }
    $( '#hidden-index' ).val( indexes_text );
  } else {
    // no index GET argument; use path.
    var str = window.location.pathname;//document.URL;
    var ret;
    if( str.charAt(str.length-1) == "/" ) {
      ret = str;
    } else {
      var last_index = str.lastIndexOf("/");
      ret = str.substr(0, last_index+1);
    }
    indexes = [ ret ];

    // we don't need to send index=
    $( '#hidden-index' ).remove();
  }

  // Set up form change listener.
  //$( '#query' ).change( function() { new_search(); do_something(); } )
  $( '#query' ).change( function() {
    $( '#hidden-query' ).val( $( '#query' ).val() );
    $( '#query-form' ).trigger('submit');
  } );
  $( '#query-form' ).attr( "action", window.location.pathname );

  // Set up document and matches slickgrids.
  /*for (var i = 0; i < 500; i++) {
    documents[i] = {
      index: "Index " + i,
      info: "Document " + i
    };
  }
  for (var i = 0; i < 500; i++) {
    matches[i] = {
      index: "Index " + i,
      pattern: "Pattern " + i,
      count: i,
      cost: i,
      viz: false,
      docs: false
    };
  }*/


  documents_grid = new Slick.Grid($("#documents-grid"), [ ], documents_columns, slickoptions);
  matches_grid = new Slick.Grid($("#matches-grid"), [ ], matches_columns, slickoptions);

  matches_grid.onSelectedRowsChanged = select_matches;

  function sgwidth(sg) {
    var cols = sg.getColumns();
    var width = 0;
    for( var i = 0; i < cols.length; i++ ) {
      width += cols[i].width;
    }
    return width + 25;
  }
  // Get the size of each slickgrid.
  var dwidth = sgwidth(documents_grid);
  var mwidth = sgwidth(matches_grid);

  var select_title = "Matches <button id=\"allmatches\">Select All</button> <button id=\"nomatches\">Select None</button>";
  //var docs_title = "<span style=\"height:16px;width:16px\"> <img id=\"docspinner\" src=\"femto-js/spinner.gif\" /> </span> Documents <span id=\"docprogress\"> ? </span> <button id=\"moredocuments\">More</button>";
  var docs_title = "<table border=\"0\"><tr><td width=\"16\"><img id=\"docspinner\" src=\"femto-js/spinner.gif\" /></td><td>Documents</td><td width=\"120\"><span id=\"docprogress\"> ? </span> </td><td><button id=\"moredocuments\">More</button></td><td><button id=\"popupdocuments\">Popup</button></td></tr></table>";

  // Fix resizing of these modal dialogs so slickgrids also resize.
  $( "#matches-dialog" ).dialog({width: mwidth, position: ["right", "top"],
                                 title: select_title,
                                 resizeStop: resize_handler(matches_grid) });
  $( "#documents-dialog" ).dialog({width: dwidth, position: ["left", "bottom"],
                                   title: docs_title,
                                   resizeStop: resize_handler(documents_grid) });

  $( "#allmatches" ).button();
  $( "#allmatches" ).click(function() {
    for( var m in matches ) matches[m].selected = true;
    display_matches();
    display_documents();
    display();
  });
  $( "#nomatches" ).button();
  $( "#nomatches" ).click(function() {
    for( var m in matches ) matches[m].selected = false;
    display_matches();
    display_documents();
    display();
  });
  $( "#moredocuments" ).button( );
  $( "#moredocuments" ).button("enable");
  $( "#docspinner" ).hide( );
  $( "#moredocuments" ).click(load_more_documents);
  $( "#popupdocuments" ).button( );
  $( "#popupdocuments" ).button("enable");
  $( "#popupdocuments" ).click( function() {
    var title = "Documents for " + selected_matches_string("","");
    var w = window.open("", "ResultsWindow", "width=600,height=600");
    if( w ) {
      w.document.open();
      w.document.write("<html><head><title>" + title + "</title></head><body>");
      w.document.write("<div class=\"f-query\"># Query " + query + "</div>\n");
      w.document.write("<div class=\"f-matches\"># Matches " + total_matches + "</div\n");
      for( var m in matches ) {
        var match = matches[m];
        if( match.selected ) {
          w.document.write("<div class=\"f-pattern\"># Pattern " + match.match + "\n");
          for( var index in match.perindex ) {
            var pi = match.perindex[index];
            w.document.write("<div class=\"f-index\"># Index " + index + " matched rows " + pi.range[0] + " to " + pi.range[1] + "\n");
            if( pi.documents ) {
              w.document.write("<div class=\"f-have\"># Showing rows " + pi.documents.range[0] + " to " + pi.documents.range[1] + "\n");
              for( var info in pi.documents.docs ) {
                var rec = pi.documents.docs[info];
                w.document.write("<div class=\"f-document\" style=\"white-space:nowrap\">" + rec.info);
                w.document.write(rec.info);
                if( rec.offsets ) {
                  for( var i=0; i < rec.offsets.length; i++ ) {
                    w.document.write(" " + rec.offsets[i]);
                  }
                }
                w.document.write("</div>\n");
              }
              w.document.write("</div>\n");
            }
            w.document.write("</div>\n");
          }
          w.document.write("</div>\n");
        }
      }
      w.document.write("</body></html>\n");
      w.document.close();
    }
  });


  $( "#viz-all" ).button();
  $( "#viz-all" ).click(function() {
    zoomed = { };
    display();
  });

  $( "#show-matches" ).button();
  $( "#show-matches" ).click(function() {
    var elt = $( "#matches-dialog" );
    if( elt.dialog("isOpen") ) elt.dialog("close");
    else {
      open_dialog_in_window(elt);
    }
  });
  $( "#show-documents" ).button();
  $( "#show-documents" ).click(function() {
    var elt = $( "#documents-dialog" );
    if( elt.dialog("isOpen") ) elt.dialog("close");
    else {
      open_dialog_in_window(elt);
      for( var m in matches ) {
        var v = matches[m];
        if( v.selected ) {
          var hasdocs = true;
          for( var index in v.perindex ) {
            if( ! v.perindex[index].documents ) hasdocs = false;
          }
          if( ! hasdocs ) add_work(v, UPDATE_DOCUMENTS);
        }
      }
      display_documents();
      do_something();
    }
  });



  $( "#viz-breadcrumbs" ).hide();
  $( "#tooltip" ).hide();
  $( "#spinner" ).hide();

  matches_grid.resizeCanvas();
  documents_grid.resizeCanvas();

  // Re-display when the window is resized.
  $(window).resize(function() {
    if( state != STATE_BLANK ) display();
  });

  // Draw some SVG stuff for experimentation.
  function drawIntro(thesvg) { 
    svg = thesvg;
  }

  $( "#svgcanvas" ).svg({onLoad: drawIntro});

  // Focus on the search box.
  $( "#query" ).focus();

  if( $( '#query' ).val() ) {
    new_search();
  }

  //window.setInterval(do_something, 1000);
  do_something();

})

