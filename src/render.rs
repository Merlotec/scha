use image::{ImageBuffer, Rgb, Rgba};
use nalgebra::{Vector2, Vector3};
use std::f64::consts::PI;
use crate::assign::Circle;

fn vector_to_rgb(vec: Vector3<f32>) -> Rgb<u8> {
    let r = (vec.x.clamp(0.0, 1.0) * 255.0).round() as u8;
    let g = (vec.y.clamp(0.0, 1.0) * 255.0).round() as u8;
    let b = (vec.z.clamp(0.0, 1.0) * 255.0).round() as u8;
    Rgb([r, g, b])
}

/// Draws the given circles to a PNG image at `output_path`. The image will be
/// width x height, and the circles will be normalized to fill the image as much as possible.
pub fn draw_circles_to_png(circles: &[Circle], width: u32, height: u32, output_path: &str) {
    if circles.is_empty() {
        let img = ImageBuffer::from_fn(width, height, |_x, _y| Rgb([255u8, 255u8, 255u8]));
        img.save(output_path).unwrap();
        return;
    }

    // Compute bounding box
    let (min_x, min_y, max_x, max_y) = {
        let mut min_x = f64::INFINITY;
        let mut min_y = f64::INFINITY;
        let mut max_x = f64::NEG_INFINITY;
        let mut max_y = f64::NEG_INFINITY;

        for c in circles {
            let x0 = c.origin.x - c.r;
            let x1 = c.origin.x + c.r;
            let y0 = c.origin.y - c.r;
            let y1 = c.origin.y + c.r;

            if x0 < min_x { min_x = x0; }
            if x1 > max_x { max_x = x1; }
            if y0 < min_y { min_y = y0; }
            if y1 > max_y { max_y = y1; }
        }

        (min_x, min_y, max_x, max_y)
    };

    if (max_x - min_x).abs() < 1e-14 || (max_y - min_y).abs() < 1e-14 {
        // Degenerate case: all circles might be in one point.
        let img = ImageBuffer::from_fn(width, height, |_x, _y| Rgb::<u8>([255,255,255]));
        img.save(output_path).unwrap();
        return;
    }

    // Compute scale and offset
    let bbox_width = max_x - min_x;
    let bbox_height = max_y - min_y;
    let scale_x = (width as f64) / bbox_width;
    let scale_y = (height as f64) / bbox_height;
    let scale = scale_x.min(scale_y);

    let scaled_width = bbox_width * scale;
    let scaled_height = bbox_height * scale;

    let x_offset = (width as f64 - scaled_width) / 2.0;
    let y_offset = (height as f64 - scaled_height) / 2.0;

    let to_image_coords = |p: Vector2<f64>| -> (f64, f64) {
        let x_img = (p.x - min_x) * scale + x_offset;
        let y_img = (p.y - min_y) * scale + y_offset;
        (x_img, y_img)
    };

    // Assign colors to each circle.
    // For simplicity, let's generate some distinct colors.
    // In practice, you might choose a palette or random colors.
    let c0 = Vector3::new(1.0, 0.0, 0.0);
    let c1 = Vector3::new(0.0, 0.0, 1.0);

    let len = circles.len() as f32;

    // Transform circles to image coordinates
    let transformed_circles: Vec<((f64, f64), f64, Vector3<f32>)> = circles.iter().enumerate()
        .map(|(i, c)| {
            let (cx, cy) = to_image_coords(c.origin);
            let s = i as f32 / len;
            ((cx, cy), c.r * scale, c0 * s + c1 * (1.0 - s))
        }).collect();

    let mut img = ImageBuffer::from_fn(width, height, |_x, _y| Rgb([255u8, 255u8, 255u8]));

    // Drawing logic:
    // The first circle is on top. That means we should check circles in order:
    // For each pixel, we check from the first (top) circle down to the last (bottom) circle.
    // Once we find a circle that the pixel is inside, we color it and stop checking further.
    for y in 0..height {
        for x in 0..width {
            let px = x as f64 + 0.5;
            let py = y as f64 + 0.5;

            // Since the first circle is on top, we check from first to last
            for ((cx, cy), r_scaled, col) in &transformed_circles {
                let dx = px - cx;
                let dy = py - cy;
                if dx*dx + dy*dy <= r_scaled*r_scaled {
                    img.put_pixel(x, y, vector_to_rgb(*col));
                    break; // Stop checking other circles
                }
            }
        }
    }

    img.save(output_path).unwrap();
}
