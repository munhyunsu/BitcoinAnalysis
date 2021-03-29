var p2pkh = document.getElementById("addrtype_p2pkh_value").getAttribute('name');
var p2sh = document.getElementById("addrtype_p2sh_value").getAttribute('name');
var bech32 = document.getElementById("addrtype_bech32_value").getAttribute('name');

var type_box = [];
if(p2pkh != '0') {
	type_box.push({'y':p2pkh, 'label':'P2PKH'});
}
if(p2sh != '0') {
	type_box.push({'y':p2sh, 'label':'P2SH'});
}
if(bech32 != '0') {
	type_box.push({'y':bech32, 'label':'Bech32'});
}

var random_color = new Array();
for(var j = 0; j < 3; j++) {
	var rd_str = "rgb(";
	for(var k = 0; k < 3; k++) {
		rd_str += (Math.floor(Math.random() * 255) + 1).toString() + ", ";
	}
	rd_str += "0.5)";
	random_color[j] = rd_str;
}

window.onload = function() {
	CanvasJS.addColorSet("custom_color", ['#BBD1E8', '#82D580', '#FF4000'])
	var chart = new CanvasJS.Chart("chartContainer", {
		colorSet: "custom_color",
			backgroundColor: "#F2F2F2",
			animationEnabled: true,
			data: [{
					type: "pie",
					startAngle: 240,
					indexLabelFontSize: 18,
					yValueFormatString: "##0\"EA\"",
					indexLabel: "{label} - {y}",
					dataPoints: type_box
			}]
	});
	chart.render();
}
