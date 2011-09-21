var deleteStatus = {}
var resizeStatus = {}
    
function toggleDelete( element )
{
    var elementId = element.attr('id')
    if ( elementId in deleteStatus )
    {
        element.css({'text-decoration': 'none'});
        delete deleteStatus[elementId];
    }
    else
    {
        element.css({'text-decoration': 'line-through'});
        deleteStatus[elementId] = "deleted";
    }
}

function reWeight( element, multiplier )
{
    var elementId = element.attr('id')
    var newSize = 100.0 * multiplier
    if ( elementId in resizeStatus )
    {
        newSize = resizeStatus[elementId] * multiplier;
    }
    resizeStatus[elementId] = newSize

    var sizeString = newSize.toString() + "%";
    element.css('font-size', sizeString);
}


function enableSelectable()
{    
    $(".selectable").hover( function()
    {
        $(".selected").removeClass("selected")
        $("#controls").remove()
        $(this).addClass("selected")
        $(this).append( '<div id="controls"><img id="delete" src="/public/images/delete.png"/><img id="increment" src="/public/images/plus.png"/><img id="decrement" src="/public/images/minus.png"/></div>' )
        var element = $(this)
        $("#delete").click( function() { toggleDelete(element); } )
        $("#increment").click( function() { reWeight(element, 1.05); } )
        $("#decrement").click( function() { reWeight(element, 1.0/1.05); } )
    } );
}

